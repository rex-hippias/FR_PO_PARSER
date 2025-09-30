# app.py
import os
import json
import uuid
import subprocess
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

app = FastAPI(title="FR PO Parser Orchestrator")

# ---------- config ----------
RUNS_BASE = "/tmp"
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://fr-po-parser.onrender.com").rstrip("/")

# ---------- models ----------
class RunRequest(BaseModel):
    run_id: str = Field(..., description="Client-supplied run identifier")
    input_urls: List[str] = Field(..., description="List of PDF URLs to download and parse")

class RunStatus(BaseModel):
    job_id: str
    run_id: str
    status: str                 # queued|running|succeeded|failed
    error: Optional[str] = None
    outputs: Optional[Dict] = None

# ---------- in-memory jobs (repopulated from disk as needed) ----------
jobs: Dict[str, Dict] = {}

# ---------- helpers ----------
def run_dir_for(run_id: str) -> str:
    return os.path.join(RUNS_BASE, run_id)

def ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)

def mk_url(abs_path: str) -> str:
    # build an absolute /download link with a url-encoded filesystem path
    return f"{PUBLIC_BASE_URL}/download?path={quote(abs_path, safe='')}"

def job_file(run_id: str) -> str:
    return os.path.join(run_dir_for(run_id), "job.json")

def save_job(job: Dict) -> None:
    try:
        with open(job_file(job["run_id"]), "w") as f:
            json.dump(job, f)
    except Exception:
        # non-fatal
        pass

def load_job_by_id(job_id: str) -> Optional[Dict]:
    # scan RUNS_BASE for job.json that matches this job_id
    if not os.path.isdir(RUNS_BASE):
        return None
    for run_id in os.listdir(RUNS_BASE):
        jf = job_file(run_id)
        try:
            if os.path.exists(jf):
                with open(jf) as f:
                    j = json.load(f)
                if j.get("job_id") == job_id:
                    return j
        except Exception:
            continue
    return None

def build_outputs(run_id: str) -> Dict:
    rdir       = run_dir_for(run_id)
    output_dir = os.path.join(rdir, "output")
    logs_dir   = os.path.join(rdir, "logs")
    debug_dir  = os.path.join(rdir, "debug")

    combined_path = os.path.join(output_dir, f"combined_{run_id}.csv")
    stdout_path   = os.path.join(logs_dir, "agent.stdout.txt")
    stderr_path   = os.path.join(logs_dir, "agent.stderr.txt")

    debug_urls: List[str] = []
    if os.path.isdir(debug_dir):
        for name in sorted(os.listdir(debug_dir)):
            if name.lower().endswith(".txt"):
                debug_urls.append(mk_url(os.path.join(debug_dir, name)))

    return {
        "combined_csv": combined_path if os.path.exists(combined_path) else None,
        "combined_csv_url": mk_url(combined_path) if os.path.exists(combined_path) else None,
        "stdout_url": mk_url(stdout_path) if os.path.exists(stdout_path) else None,
        "stderr_url": mk_url(stderr_path) if os.path.exists(stderr_path) else None,
        "debug_urls": debug_urls,
    }

def download_to(url: str, dest_path: str) -> None:
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download {url}: {e}")
    with open(dest_path, "wb") as f:
        f.write(r.content)

# ---------- routes ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.post("/runs_async")
def start_run(req: RunRequest):
    if not req.input_urls:
        raise HTTPException(status_code=422, detail="input_urls must be non-empty")

    job_id = "job-" + uuid.uuid4().hex[:10]
    rdir = run_dir_for(req.run_id)
    input_dir = os.path.join(rdir, "input")
    parsed_dir = os.path.join(rdir, "parsed")
    output_dir = os.path.join(rdir, "output")
    logs_dir   = os.path.join(rdir, "logs")
    debug_dir  = os.path.join(rdir, "debug")
    ensure_dirs(input_dir, parsed_dir, output_dir, logs_dir, debug_dir)

    # download inputs
    for url in req.input_urls:
        fname = os.path.basename(url.split("?")[0]) or "input.pdf"
        dest = os.path.join(input_dir, fname)
        download_to(url, dest)

    # record job (A/C)
    jobs[job_id] = {"job_id": job_id, "status": "running", "run_id": req.run_id}
    save_job(jobs[job_id])

    # run the agent (synchronously here)
    try:
        result = subprocess.run(
            [
                "python", "run_agent.py",
                "--run-id", req.run_id,
                "--input",  input_dir,
                "--parsed", parsed_dir,
                "--output", output_dir,
                "--logs",   logs_dir,
            ],
            capture_output=True,
            text=True,
        )

        # gather outputs and finalize job
        outputs = build_outputs(req.run_id)
        jobs[job_id]["outputs"] = outputs
        if result.returncode == 0:
            jobs[job_id]["status"] = "succeeded"
            jobs[job_id]["error"]  = None
        else:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"]  = (result.stderr or "").strip() or f"agent exited {result.returncode}"

        save_job(jobs[job_id])  # (D) persist final state

    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"]  = str(e)
        jobs[job_id]["outputs"] = build_outputs(req.run_id)
        save_job(jobs[job_id])

    return {"job_id": job_id, "run_id": req.run_id, "status": jobs[job_id]["status"]}

@app.get("/runs/{job_id}")
def get_status(job_id: str):
    # (E) serve from memory if present; otherwise, reconstruct from disk
    if job_id not in jobs:
        j = load_job_by_id(job_id)
        if not j:
            raise HTTPException(status_code=404, detail="unknown job_id")

        # recompute outputs and naive status if needed
        run_id = j["run_id"]
        outputs = build_outputs(run_id)
        j["outputs"] = outputs
        if not j.get("status"):
            j["status"] = "succeeded" if outputs.get("combined_csv_url") else "failed"

        jobs[job_id] = j  # repopulate memory for subsequent requests

    return jobs[job_id]

@app.get("/download")
def download_file(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="file not found")
    return FileResponse(path)
