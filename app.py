# app.py
import os
import io
import json
import uuid
import shutil
import queue
import threading
import subprocess
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from pydantic import BaseModel, HttpUrl

# ---------- Config ----------
APP_VERSION = os.getenv("APP_VERSION", "1.3.2")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")  # e.g. https://fr-po-parser.onrender.com
WORKDIR_BASE = os.getenv("WORKDIR_BASE", "/tmp")
AGENT_CMD = os.getenv("AGENT_CMD")  # optional, overrides the default run command

# ---------- FastAPI ----------
app = FastAPI(title="FR PO Parser API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

# ---------- Models ----------
class RunBody(BaseModel):
    run_id: Optional[str] = None
    input_urls: List[HttpUrl]
    output_prefix: Optional[str] = None
    callback_url: Optional[HttpUrl] = None  # optional webhook back to n8n

class JobStatus(BaseModel):
    job_id: str
    run_id: str
    status: str  # queued | running | succeeded | failed
    error: Optional[str] = None
    outputs: Optional[Dict[str, Any]] = None

# ---------- In-memory state + persistence ----------
_jobs: Dict[str, JobStatus] = {}
_q: "queue.Queue[Dict[str, Any]]" = queue.Queue()

def _job_state_path(job_id: str) -> str:
    return os.path.join(WORKDIR_BASE, f"{job_id}.json")

def _save_job(data: Dict[str, Any]) -> None:
    try:
        with open(_job_state_path(data["job_id"]), "w") as f:
            json.dump(data, f)
    except Exception:
        pass

def _load_job(job_id: str) -> Optional[Dict[str, Any]]:
    p = _job_state_path(job_id)
    if os.path.exists(p):
        try:
            with open(p, "r") as f:
                return json.load(f)
        except Exception:
            return None
    return None

# ---------- Helpers ----------
def make_workdir(run_id: str) -> str:
    w = os.path.join(WORKDIR_BASE, run_id)
    os.makedirs(os.path.join(w, "input"), exist_ok=True)
    os.makedirs(os.path.join(w, "parsed"), exist_ok=True)
    os.makedirs(os.path.join(w, "output"), exist_ok=True)
    os.makedirs(os.path.join(w, "logs"), exist_ok=True)
    return w

def download_to(dest_path: str, url: str) -> None:
    # Simple streaming downloader
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

def _attach_urls(request: Request, outputs: Dict[str, Any]) -> Dict[str, Any]:
    """Attach *_url keys for any known file paths."""
    out = dict(outputs or {})
    base = PUBLIC_BASE_URL or str(request.base_url).rstrip("/")
    def add_url(key: str):
        path = out.get(key)
        if isinstance(path, str) and os.path.isabs(path):
            out[f"{key}_url"] = f"{base}/download?path={requests.utils.quote(path, safe='')}"
    for k in ("combined_csv", "changelog", "stdout", "stderr"):
        add_url(k)
    return out

def _failure_logs_for(run_id: str) -> Dict[str, str]:
    w = os.path.join(WORKDIR_BASE, run_id)
    return {
        "stdout": os.path.join(w, "logs", "agent.stdout.txt"),
        "stderr": os.path.join(w, "logs", "agent.stderr.txt"),
    }

# ---------- Agent runner ----------
def run_agent_sync(run_id: str, input_urls: List[str]) -> Dict[str, Any]:
    """Download inputs, run the agent process, capture logs, return paths."""
    workdir = make_workdir(run_id)
    stdout_path = os.path.join(workdir, "logs", "agent.stdout.txt")
    stderr_path = os.path.join(workdir, "logs", "agent.stderr.txt")
    os.makedirs(os.path.dirname(stdout_path), exist_ok=True)

    def _append_stderr(msg: str):
        os.makedirs(os.path.dirname(stderr_path), exist_ok=True)
        with open(stderr_path, "a") as f:
            f.write(str(msg).rstrip() + "\n")

    # Validate & log early issues into stderr so failures always have content
    if not input_urls:
        _append_stderr("Validation error: input_urls must be non-empty")
        raise ValueError("input_urls must be non-empty")

    # Download inputs
    try:
        for url in input_urls:
            filename = (url.split("/")[-1].split("?")[0]) or f"file-{uuid.uuid4().hex[:6]}.pdf"
            dest = os.path.join(workdir, "input", filename)
            download_to(dest, url)
    except Exception as e:
        _append_stderr(f"Download error: {e}")
        raise

    # Build command
    if AGENT_CMD:
        cmd = AGENT_CMD.split()
    else:
        cmd = [
            "python", "run_agent.py",
            "--run-id", run_id,
            "--input", os.path.join(workdir, "input"),
            "--parsed", os.path.join(workdir, "parsed"),
            "--output", os.path.join(workdir, "output"),
            "--logs", os.path.join(workdir, "logs"),
        ]

    # Execute
    res = subprocess.run(cmd, cwd=".", text=True, capture_output=True)
    # Persist logs
    with open(stdout_path, "w") as f:
        f.write(res.stdout or "")
    with open(stderr_path, "a") as f:
        f.write(res.stderr or "")

    if res.returncode != 0:
        raise RuntimeError(f"agent exited {res.returncode}")

    # Expected outputs (adjust to your agent’s behavior if needed)
    outputs = {
        "combined_csv": os.path.join(workdir, "output", f"combined_{run_id}.csv"),
        "changelog": os.path.join(workdir, "CHANGELOG.md"),
        "stdout": stdout_path,
        "stderr": stderr_path,
    }
    return outputs

# ---------- Worker thread ----------
def _post_callback(url: str, payload: dict) -> None:
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception:
        # best-effort callback; ignore failures
        pass

def _worker_loop():
    while True:
        job = _q.get()
        job_id = job["job_id"]
        try:
            st = _jobs.get(job_id) or JobStatus(job_id=job_id, run_id=job["run_id"], status="queued")
            st = JobStatus(**{**st.dict(), "status": "running"})
            _jobs[job_id] = st; _save_job(st.dict())

            outputs = run_agent_sync(job["run_id"], job["input_urls"])

            st = JobStatus(job_id=job_id, run_id=job["run_id"], status="succeeded", outputs=outputs)
            _jobs[job_id] = st; _save_job(st.dict())

        except Exception as e:
            # Always include log paths & append the exception into stderr
            fail_outputs = _failure_logs_for(job["run_id"])
            try:
                os.makedirs(os.path.dirname(fail_outputs["stderr"]), exist_ok=True)
                with open(fail_outputs["stderr"], "a") as f:
                    f.write(f"[worker] {e}\n")
            except Exception:
                pass
            st = JobStatus(job_id=job_id, run_id=job["run_id"], status="failed", error=str(e), outputs=fail_outputs)
            _jobs[job_id] = st; _save_job(st.dict())
        finally:
            # Fire callback if provided
            try:
                cb = job.get("callback_url")
                if cb:
                    # Attach URLs in callback too
                    payload = _jobs[job_id].dict()
                    # Build absolute URLs from PUBLIC_BASE_URL if present
                    if payload.get("outputs"):
                        if PUBLIC_BASE_URL:
                            outs = payload["outputs"]
                            for k in ("combined_csv", "changelog", "stdout", "stderr"):
                                p = outs.get(k)
                                if p and os.path.isabs(p):
                                    outs[f"{k}_url"] = f"{PUBLIC_BASE_URL}/download?path={requests.utils.quote(p, safe='')}"
                    _post_callback(cb, payload)
            except Exception:
                pass
            _q.task_done()

_thread = threading.Thread(target=_worker_loop, daemon=True)
_thread.start()

# ---------- Routes ----------
@app.get("/")
def root():
    return {"ok": True, "service": "FR PO Parser", "version": APP_VERSION}

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/runs_async")
def start_run_async(body: RunBody):
    run_id = body.run_id or f"RUN-{uuid.uuid4().hex[:10]}"
    job_id = f"job-{uuid.uuid4().hex[:10]}"

    # Store an initial state
    st = JobStatus(job_id=job_id, run_id=run_id, status="queued")
    _jobs[job_id] = st
    _save_job(st.dict())

    _q.put({
        "job_id": job_id,
        "run_id": run_id,
        "input_urls": [str(u) for u in body.input_urls],
        "callback_url": str(body.callback_url) if body.callback_url else None,
    })
    return {"job_id": job_id, "run_id": run_id, "status": "queued"}

@app.get("/runs/{job_id}")
def get_run_status(job_id: str, request: Request):
    try:
        st = _jobs.get(job_id)
        if not st:
            data = _load_job(job_id)
            if data:
                st = JobStatus(**data)
            else:
                return JSONResponse(status_code=404, content={"error": "unknown job_id"})

        payload = st.dict()
        if st.outputs:
            try:
                payload["outputs"] = _attach_urls(request, st.outputs)
            except Exception as e:
                payload["outputs"] = st.outputs
                payload["attach_error"] = str(e)
        return payload

    except Exception as e:
        data = _load_job(job_id)
        safe_state = data if isinstance(data, dict) else None
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "detail": str(e),
                "job_id": job_id,
                "state": safe_state,
            },
        )

@app.get("/download")
def download(path: str = Query(..., description="Absolute file path produced by the job")):
    if not path or not os.path.isabs(path):
        raise HTTPException(status_code=400, detail="path must be an absolute file path")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="file not found")
    # Guess filename from path
    filename = os.path.basename(path)
    return FileResponse(path, filename=filename)

# ---------- Local run ----------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
