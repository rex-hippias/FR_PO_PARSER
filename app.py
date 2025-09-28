# app.py
import os
import json
import uuid
import queue
import threading
import subprocess
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel, HttpUrl

# =========================
# Config
# =========================
APP_VERSION = os.getenv("APP_VERSION", "1.4.0")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")  # e.g. https://fr-po-parser.onrender.com
WORKDIR_BASE = os.getenv("WORKDIR_BASE", "/tmp")
AGENT_CMD = os.getenv("AGENT_CMD")  # optional override for run command
SMOKE_SUCCESS = os.getenv("SMOKE_SUCCESS", "0") == "1"  # Step 1: force success CSV

# =========================
# FastAPI
# =========================
app = FastAPI(title="FR PO Parser API", version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_headers=["*"],
    allow_methods=["*"],
)

# =========================
# Models
# =========================
class RunBody(BaseModel):
    run_id: Optional[str] = None
    input_urls: List[HttpUrl]
    output_prefix: Optional[str] = None
    callback_url: Optional[HttpUrl] = None  # optional webhook

class JobStatus(BaseModel):
    job_id: str
    run_id: str
    status: str  # queued | running | succeeded | failed
    error: Optional[str] = None
    outputs: Optional[Dict[str, Any]] = None

# =========================
# State & persistence
# =========================
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

# =========================
# Helpers
# =========================
def make_workdir(run_id: str) -> str:
    w = os.path.join(WORKDIR_BASE, run_id)
    os.makedirs(os.path.join(w, "input"), exist_ok=True)
    os.makedirs(os.path.join(w, "parsed"), exist_ok=True)
    os.makedirs(os.path.join(w, "output"), exist_ok=True)
    os.makedirs(os.path.join(w, "logs"), exist_ok=True)
    return w

def download_to(dest_path: str, url: str) -> None:
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    os.makedirs(os.path.dirname(dest_path), exist_ok=True
                )
    with open(dest_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

def _attach_urls(request: Request, outputs: Dict[str, Any]) -> Dict[str, Any]:
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

# =========================
# Core runner
# =========================
def run_agent_sync(run_id: str, input_urls: List[str]) -> Dict[str, Any]:
    """
    Downloads inputs, runs the agent, captures logs, returns file paths.

    Step 1 (SMOKE_SUCCESS): if SMOKE_SUCCESS=1, write a tiny CSV and succeed.
    Step 2 (CSV fallback): if expected combined CSV doesn't exist,
                           return the first *.csv found in output/.
    """
    import glob

    workdir = make_workdir(run_id)
    stdout_path = os.path.join(workdir, "logs", "agent.stdout.txt")
    stderr_path = os.path.join(workdir, "logs", "agent.stderr.txt")
    os.makedirs(os.path.dirname(stdout_path), exist_ok=True)

    def _append_stderr(msg: str):
        os.makedirs(os.path.dirname(stderr_path), exist_ok=True)
        with open(stderr_path, "a") as f:
            f.write(str(msg).rstrip() + "\n")

    # Validate early, always write to stderr on failure
    if not input_urls:
        _append_stderr("Validation error: input_urls must be non-empty")
        raise ValueError("input_urls must be non-empty")

    # Download
    try:
        for url in input_urls:
            filename = (url.split("/")[-1].split("?")[0]) or f"file-{uuid.uuid4().hex[:6]}.pdf"
            dest = os.path.join(workdir, "input", filename)
            download_to(dest, url)
    except Exception as e:
        _append_stderr(f"Download error: {e}")
        raise

    # ---- Step 1: SMOKE_SUCCESS switch ----
    if SMOKE_SUCCESS:
        csv_path = os.path.join(workdir, "output", f"combined_{run_id}.csv")
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        with open(csv_path, "w") as f:
            f.write("po_number,line_number,sku,qty,price\n")
            f.write("TEST-PO,1,SKU-TEST,1,0.00\n")
        with open(stdout_path, "w") as f:
            f.write("smoke_success: created dummy CSV\n")
        with open(stderr_path, "a") as f:
            f.write("")
        return {
            "combined_csv": csv_path,
            "changelog": os.path.join(workdir, "CHANGELOG.md"),
            "stdout": stdout_path,
            "stderr": stderr_path,
        }
    # --------------------------------------

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

    # Execute the external agent
    res = subprocess.run(cmd, cwd=".", text=True, capture_output=True)

    # Persist logs
    with open(stdout_path, "w") as f:
        f.write(res.stdout or "")
    with open(stderr_path, "a") as f:
        f.write(res.stderr or "")

    if res.returncode != 0:
        raise RuntimeError(f"agent exited {res.returncode}")

    # ---- Step 2: CSV fallback ----
    csv_expected = os.path.join(workdir, "output", f"combined_{run_id}.csv")
    csv_path = csv_expected
    if not os.path.exists(csv_expected):
        # find any .csv in output/
        try:
            candidates = sorted(glob.glob(os.path.join(workdir, "output", "*.csv")))
            if candidates:
                csv_path = candidates[0]
        except Exception as e:
            _append_stderr(f"CSV fallback glob error: {e}")
    # --------------------------------

    # (Optional) help debug output dir contents on server
    try:
        import os as _os
        listing = "\n".join(sorted(_os.listdir(os.path.join(workdir, "output"))))
        with open(stderr_path, "a") as f:
            f.write(f"[runner] output dir listing:\n{listing}\n")
    except Exception:
        pass

    return {
        "combined_csv": csv_path,
        "changelog": os.path.join(workdir, "CHANGELOG.md"),
        "stdout": stdout_path,
        "stderr": stderr_path,
    }

# =========================
# Worker thread
# =========================
def _post_callback(url: str, payload: dict) -> None:
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception:
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
            try:
                cb = job.get("callback_url")
                if cb:
                    payload = _jobs[job_id].dict()
                    # attach absolute URLs when possible
                    outs = payload.get("outputs")
                    if outs and PUBLIC_BASE_URL:
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

# =========================
# Routes
# =========================
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
    filename = os.path.basename(path)
    return FileResponse(path, filename=filename)

# =========================
# Local dev
# =========================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
