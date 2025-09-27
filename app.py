# app.py
# FastAPI API for PO parser: sync (/runs) and async (/runs_async + /runs/{id})
# with file-backed job status and downloadable agent logs.
from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl
from typing import List, Optional, Dict, Any
import os, uuid, shutil, subprocess, logging, pathlib, threading, queue, json, textwrap
import requests

# ---------- Config ----------
WORKDIR_BASE = os.getenv("WORKDIR_BASE", "/tmp")   # Render has write access here
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# ---------- App ----------
app = FastAPI(title="PO Agent API", version="1.3.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("po-agent")

# ---------- Models ----------
class RunBody(BaseModel):
    run_id: Optional[str] = None
    input_urls: List[HttpUrl]                  # Dropbox direct links (?dl=1)
    output_prefix: Optional[str] = None        # reserved for future (e.g., s3://...)

class JobStatus(BaseModel):
    job_id: str
    run_id: str
    status: str                                # queued | running | succeeded | failed
    error: Optional[str] = None
    outputs: Optional[Dict[str, Any]] = None   # filled when ready

# ---------- Helpers ----------
def make_workdir(run_id: str) -> str:
    workdir = os.path.join(WORKDIR_BASE, run_id)
    for sub in ("input", "parsed", "output", "logs"):
        os.makedirs(os.path.join(workdir, sub), exist_ok=True)
    return workdir

def _is_dropbox_direct(url: str) -> bool:
    return "dropbox.com" in url and "dl=1" in url

def download_to(path: str, url: str) -> None:
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            shutil.copyfileobj(r.raw, f)

def safe_local_path(p: str) -> str:
    base = pathlib.Path(WORKDIR_BASE).resolve()
    target = pathlib.Path(p).resolve()
    if not str(target).startswith(str(base)):
        raise ValueError("Requested path is outside allowed base directory.")
    return str(target)

def build_download_url(request: Request, local_path: str) -> str:
    return str(request.url_for("download") + f"?path={local_path}")

def run_agent_sync(run_id: str, input_urls: List[str]) -> Dict[str, Any]:
    """
    Download inputs, run agent synchronously, and return outputs dict.
    Raises on failure (with stderr tail in message) but also writes stdout/stderr to files.
    """
    workdir = make_workdir(run_id)

    if not input_urls:
        raise ValueError("input_urls must be non-empty")

    # Download inputs
    for url in input_urls:
        if "dropbox.com" in url and not _is_dropbox_direct(str(url)):
            log.warning(f"URL missing dl=1: {url}")
        filename = str(url).split("/")[-1].split("?")[0] or f"file-{uuid.uuid4().hex[:6]}.pdf"
        dest = os.path.join(workdir, "input", filename)
        log.info(f"[DOWNLOAD] {url} -> {dest}")
        download_to(dest, str(url))

    # Execute agent
    cmd = [
        "python", "run_agent.py",
        "--run-id", run_id,
        "--input", os.path.join(workdir, "input"),
        "--parsed", os.path.join(workdir, "parsed"),
        "--output", os.path.join(workdir, "output"),
        "--logs", os.path.join(workdir, "logs"),
    ]
    log.info(f"[EXEC] {' '.join(cmd)}")

    res = subprocess.run(
        cmd,
        cwd=".",
        text=True,
        capture_output=True,
    )

    # Persist agent logs
    stdout_path = os.path.join(workdir, "logs", "agent.stdout.txt")
    stderr_path = os.path.join(workdir, "logs", "agent.stderr.txt")
    os.makedirs(os.path.dirname(stdout_path), exist_ok=True)
    with open(stdout_path, "w") as f:
        f.write(res.stdout or "")
    with open(stderr_path, "w") as f:
        f.write(res.stderr or "")

    if res.returncode != 0:
        tail = (res.stderr or "")[-1200:]
        raise RuntimeError(f"agent exited {res.returncode}. stderr tail:\n{textwrap.dedent(tail)}")

    # Outputs expected from agent
    combined_csv = os.path.join(workdir, "output", f"combined_{run_id}.csv")
    changelog = os.path.join(workdir, "CHANGELOG.md")
    return {
        "combined_csv": combined_csv,
        "changelog": changelog,
        "stdout": stdout_path,
        "stderr": stderr_path,
    }

# ---------- File-backed job store ----------
JOBS_DIR = os.path.join(WORKDIR_BASE, "jobs")
os.makedirs(JOBS_DIR, exist_ok=True)
_jobs: Dict[str, JobStatus] = {}
_q: "queue.Queue[Dict[str, Any]]" = queue.Queue()

def _job_path(job_id: str) -> str:
    return os.path.join(JOBS_DIR, f"{job_id}.json")

def _save_job(status: Dict[str, Any]):
    try:
        with open(_job_path(status["job_id"]), "w") as f:
            json.dump(status, f)
    except Exception:
        log.exception("failed to save job status")

def _load_job(job_id: str) -> Optional[Dict[str, Any]]:
    p = _job_path(job_id)
    if not os.path.exists(p):
        return None
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        log.exception("failed to load job status")
        return None

def _worker_loop():
    while True:
        job = _q.get()  # {job_id, run_id, input_urls}
        job_id = job["job_id"]
        try:
            st = _jobs.get(job_id) or JobStatus(job_id=job_id, run_id=job["run_id"], status="queued")
            # running
            st = JobStatus(**{**st.dict(), "status": "running"})
            _jobs[job_id] = st; _save_job(st.dict())

            outputs = run_agent_sync(job["run_id"], job["input_urls"])

            # succeeded
            st = JobStatus(job_id=job_id, run_id=job["run_id"], status="succeeded", outputs=outputs)
            _jobs[job_id] = st; _save_job(st.dict())
        except Exception as e:
            st = JobStatus(job_id=job_id, run_id=job["run_id"], status="failed", error=str(e),
                           outputs=st.outputs if 'st' in locals() and st.outputs else None)
            _jobs[job_id] = st; _save_job(st.dict())
        finally:
            _q.task_done()

threading.Thread(target=_worker_loop, daemon=True).start()

# ---------- URL attachment helper (PUT THIS BEFORE get_run_status) ----------
def _attach_urls(request: Request, outputs: dict) -> dict:
    if not outputs:
        return {}
    out = dict(outputs)
    if outputs.get("combined_csv"):
        out["combined_csv_url"] = build_download_url(request, outputs["combined_csv"])
    if outputs.get("changelog"):
        out["changelog_url"] = build_download_url(request, outputs["changelog"])
    if outputs.get("stdout"):
        out["stdout_url"] = build_download_url(request, outputs["stdout"])
    if outputs.get("stderr"):
        out["stderr_url"] = build_download_url(request, outputs["stderr"])
    return out

# ---------- Routes ----------
@app.get("/")
def health() -> dict:
    return {"status": "ok", "service": "po-agent", "docs": "/docs"}

@app.post("/runs")
def start_run(body: RunBody, request: Request):
    """SYNC: runs the agent inline (may hit Render free request timeout)."""
    run_id = body.run_id or f"RUN-{uuid.uuid4().hex[:10]}"
    try:
        outputs = run_agent_sync(run_id, [str(u) for u in body.input_urls])
        result_outputs = _attach_urls(request, outputs)
        return {"run_id": run_id, "outputs": result_outputs}
    except Exception as e:
        log.exception("Run failed")
        # Try to include log URLs if we already created a workdir
        workdir = os.path.join(WORKDIR_BASE, run_id)
        stdout = os.path.join(workdir, "logs", "agent.stdout.txt")
        stderr = os.path.join(workdir, "logs", "agent.stderr.txt")
        outputs = {}
        if os.path.exists(stdout): outputs["stdout"] = stdout
        if os.path.exists(stderr): outputs["stderr"] = stderr
        return JSONResponse(
            status_code=502,
            content={
                "error": "Run failed",
                "detail": str(e),
                "run_id": run_id,
                "outputs": _attach_urls(request, outputs) if outputs else None,
            },
        )

@app.post("/runs_async")
def start_run_async(body: RunBody):
    """ASYNC: enqueue a job and return job_id immediately; poll GET /runs/{job_id}."""
    run_id = body.run_id or f"RUN-{uuid.uuid4().hex[:10]}"
    job_id = f"job-{uuid.uuid4().hex[:10]}"
    st = JobStatus(job_id=job_id, run_id=run_id, status="queued")
    _jobs[job_id] = st; _save_job(st.dict())
    _q.put({"job_id": job_id, "run_id": run_id, "input_urls": [str(u) for u in body.input_urls]})
    return {"job_id": job_id, "run_id": run_id, "status": "queued"}

@app.get("/runs/{job_id}")
def get_run_status(job_id: str, request: Request):
    st = _jobs.get(job_id)
    if not st:
        data = _load_job(job_id)
        if data:
            st = JobStatus(**data)
        else:
            return JSONResponse(status_code=404, content={"error": "unknown job_id"})
    payload = st.dict()
    if st.outputs:
        payload["outputs"] = _attach_urls(request, st.outputs)
    return payload

@app.get("/download", name="download")
def download(path: str = Query(..., description="Absolute local path returned by /runs or /runs/{id}")):
    try:
        local = safe_local_path(path)
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    if not os.path.exists(local):
        return JSONResponse(status_code=404, content={"error": "file not found", "path": local})
    filename = os.path.basename(local)
    media_type = "text/csv" if filename.lower().endswith(".csv") else "text/markdown"
    return FileResponse(local, filename=filename, media_type=media_type)
