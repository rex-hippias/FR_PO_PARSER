# app.py
import os
import uuid
import subprocess
import json, os
from typing import List, Optional, Dict

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from urllib.parse import quote

app = FastAPI(title="FR PO Parser Orchestrator")

RUNS_BASE = "/tmp"
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://fr-po-parser.onrender.com").rstrip("/")


# ---------- Models ----------
class RunRequest(BaseModel):
    run_id: str = Field(..., description="Client-supplied run identifier")
    input_urls: List[str] = Field(..., description="List of PDF URLs to download and parse")


class RunStatus(BaseModel):
    job_id: str
    run_id: str
    status: str  # queued|running|succeeded|failed
    error: Optional[str] = None
    outputs: Optional[Dict] = None


# ---------- In-memory job store (simple) ----------
jobs: Dict[str, Dict] = {}


# ---------- Helpers ----------
def run_dir_for(run_id: str) -> str:
    return os.path.join(RUNS_BASE, run_id)


def mk_url(abs_path: str) -> str:
    return f"{PUBLIC_BASE_URL}/download?path={quote(abs_path, safe='')}"


def ensure_dirs(*paths: str) -> None:
    for p in paths:
        os.makedirs(p, exist_ok=True)


def download_to(url: str, dest_path: str) -> None:
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download {url}: {e}")
    with open(dest_path, "wb") as f:
        f.write(r.content)
def job_file(run_id: str) -> str:
    return os.path.join(RUNS_BASE, run_id, "job.json")

def save_job(job: dict) -> None:
    try:
        with open(job_file(job["run_id"]), "w") as f:
            json.dump(job, f)
    except Exception:
        pass

def load_job_by_id(job_id: str) -> dict | None:
    # scan RUNS_BASE for a job.json that matches this job_id
    if not os.path.isdir(RUNS_BASE):
        return None
    for run_id in os.listdir(RUNS_BASE):
        p = job_file(run_id)
        try:
            if os.path.exists(p):
                with open(p) as f:
                    j = json.load(f)
                if j.get("job_id") == job_id:
                    return j
        except Exception:
            continue
    return None

# ---------- Routes ----------
@app.get("/health")
def health():
    return {"ok": True}


@app.post("/runs_async")
def start_run(req: RunRequest):
    job_id = "job-" + uuid.uuid4().hex[:10]
    rdir = run_dir_for(req.run_id)
    input_dir = os.path.join(rdir, "input")
    parsed_dir = os.path.join(rdir, "parsed")
    output_dir = os.path.join(rdir, "output")
    logs_dir = os.path.join(rdir, "logs")
    debug_dir = os.path.join(rdir, "debug")
    ensure_dirs(input_dir, parsed_dir, output_dir, logs_dir, debug_dir)

    # download inputs
    if not req.input_urls:
        raise HTTPException(status_code=422, detail="input_urls must be non-empty")

    for url in req.input_urls:
        fname = os.path.basename(url.split("?")[0]) or "input.pdf"
        dest = os.path.join(input_dir, fname)
        download_to(url, dest)

    jobs[job_id] = {"status": "running", "run_id": req.run_id}

    # run agent synchronously (simple worker)
    try:
        result = subprocess.run(
            [
                "python",
                "run_agent.py",
                "--run-id", req.run_id,
                "--input", input_dir,
                "--parsed", parsed_dir,
                "--output", output_dir,
                "--logs", logs_dir,
            ],
            capture_output=True,
            text=True,
        )

        # Collect outputs (paths + URLs)
        combined_path = os.path.join(output_dir, f"combined_{req.run_id}.csv")
        stdout_path = os.path.join(logs_dir, "agent.stdout.txt")
        stderr_path = os.path.join(logs_dir, "agent.stderr.txt")

        debug_urls = []
        if os.path.isdir(debug_dir):
            for name in sorted(os.listdir(debug_dir)):
                if name.lower().endswith(".txt"):
                    debug_urls.append(mk_url(os.path.join(debug_dir, name)))

        outputs = {
            "combined_csv": combined_path,
            "combined_csv_url": mk_url(combined_path) if os.path.exists(combined_path) else None,
            "stdout_url": mk_url(stdout_path) if os.path.exists(stdout_path) else None,
            "stderr_url": mk_url(stderr_path) if os.path.exists(stderr_path) else None,
            "debug_urls": debug_urls,
        }

        jobs[job_id]["outputs"] = outputs
        if result.returncode == 0:
            jobs[job_id]["status"] = "succeeded"
            jobs[job_id]["error"] = None
        else:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = (result.stderr or "").strip() or f"agent exited {result.returncode}"

    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)

    return {"job_id": job_id, "run_id": req.run_id, "status": jobs[job_id]["status"]}


@app.get("/runs/{job_id}")
def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="unknown job_id")
    return jobs[job_id]


@app.get("/download")
def download_file(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="file not found")
    # let FastAPI stream the file
    return FileResponse(path)
