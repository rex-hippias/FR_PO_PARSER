#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import json
import shlex
import uuid
import time
import glob
import subprocess
from typing import Dict, Optional, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
from starlette.responses import FileResponse, PlainTextResponse
from starlette.requests import Request
from urllib.parse import quote

# -------------------------
# Config
# -------------------------

PORT = int(os.environ.get("PORT", "10000"))
BASE_TMP = "/tmp"  # all runs live here: /tmp/<RUN-ID>/
# If deployed behind a custom domain, you can hardcode it; otherwise we infer from request.
PUBLIC_BASE_URL_ENV = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")

# -------------------------
# FastAPI & CORS
# -------------------------

app = FastAPI(title="FR PO Parser Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # tighten if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Models
# -------------------------

class RunRequest(BaseModel):
    run_id: Optional[str] = Field(default=None, description="Optional caller-supplied run id")
    input_urls: List[str] = Field(default_factory=list, description="Array of HTTP/HTTPS URLs to PDFs")

    @validator("input_urls")
    def non_empty_strings(cls, v):
        if not isinstance(v, list):
            raise ValueError("input_urls must be an array of strings")
        for i, s in enumerate(v):
            if not isinstance(s, str) or not s.strip():
                raise ValueError(f"input_urls[{i}] must be a non-empty string")
        return v


class RunStatus(BaseModel):
    job_id: str
    run_id: str
    status: str                         # queued | running | succeeded | failed
    error: Optional[str] = None
    outputs: Optional[Dict] = None      # {csv_path, csv_url, stderr_path, stderr_url, debug_urls: []}


# -------------------------
# In-memory job store
# -------------------------

JOBS: Dict[str, RunStatus] = {}          # job_id -> status model
PIDS: Dict[str, int] = {}                # job_id -> process pid (optional)


# -------------------------
# Helpers
# -------------------------

def ensure_dir(d: str) -> str:
    os.makedirs(d, exist_ok=True)
    return d


def mk_run_dirs(run_id: str) -> Dict[str, str]:
    base = os.path.join(BASE_TMP, run_id)
    paths = {
        "base": base,
        "input": os.path.join(base, "input"),
        "parsed": os.path.join(base, "parsed"),
        "output": os.path.join(base, "output"),
        "logs": os.path.join(base, "logs"),
        "debug": os.path.join(base, "debug"),
    }
    for p in paths.values():
        ensure_dir(p)
    return paths


def absolute_download_url(req: Request, local_path: str) -> str:
    # Build a fully qualified URL to our /download endpoint
    quoted = quote(local_path, safe="")
    if PUBLIC_BASE_URL_ENV:
        return f"{PUBLIC_BASE_URL_ENV}/download?path={quoted}"
    # infer from request
    base = str(req.base_url).rstrip("/")
    return f"{base}/download?path={quoted}"


def collect_outputs(req: Request, run_id: str) -> Dict:
    base = os.path.join(BASE_TMP, run_id)
    out_dir = os.path.join(base, "output")
    logs_dir = os.path.join(base, "logs")
    debug_dir = os.path.join(base, "debug")

    # CSV (there should be at most one combined_*.csv)
    csv_paths = sorted(glob.glob(os.path.join(out_dir, "combined_*.csv")))
    csv_path = csv_paths[0] if csv_paths else None
    csv_url = absolute_download_url(req, csv_path) if csv_path else None

    # stderr
    stderr_path = os.path.join(logs_dir, "agent.stderr.txt")
    stderr_url = absolute_download_url(req, stderr_path) if os.path.isfile(stderr_path) else None

    # debug text files (optional)
    debug_paths = sorted(glob.glob(os.path.join(debug_dir, "*.txt")))
    debug_urls = [absolute_download_url(req, p) for p in debug_paths] if debug_paths else []

    return {
        "csv_path": csv_path,
        "csv_url": csv_url,
        "stderr_path": stderr_path if os.path.isfile(stderr_path) else None,
        "stderr_url": stderr_url,
        "debug_urls": debug_urls,
        "output_dir": out_dir,
        "logs_dir": logs_dir,
        "debug_dir": debug_dir,
        "base_dir": base,
    }


def write_urls_manifest(input_dir: str, urls: List[str]) -> None:
    # For run_agent.py to read from disk if it wants
    manifest = os.path.join(input_dir, "_urls.json")
    with open(manifest, "w", encoding="utf-8") as f:
        json.dump(urls, f)


def launch_worker(run_id: str, paths: Dict[str, str], input_urls: List[str]) -> subprocess.Popen:
    env = os.environ.copy()
    # Also pass URLs via env in case you prefer that route inside the agent
    env["INPUT_URLS"] = json.dumps(input_urls)

    cmd = [
        "python",
        "run_agent.py",
        "--run-id", run_id,
        "--input", paths["input"],
        "--parsed", paths["parsed"],
        "--output", paths["output"],
        "--logs", paths["logs"],
        "--debug", paths["debug"],
    ]
    # Use text mode? Not needed; we only care about exit code & artifacts.
    proc = subprocess.Popen(cmd, env=env)
    return proc


# -------------------------
# Routes
# -------------------------

@app.get("/", response_class=PlainTextResponse)
def root():
    return "FR PO Parser Service is up\n"


@app.get("/healthz", response_class=PlainTextResponse)
def health():
    return "ok"


@app.post("/runs_async", response_model=RunStatus)
def runs_async(req: Request, body: RunRequest):
    # Validate
    if not body.input_urls:
        raise HTTPException(status_code=422, detail="input_urls must be non-empty")

    # Create run and paths
    run_id = body.run_id or f"RUN-{time.strftime('%Y-%m-%dT%H-%M-%S', time.localtime())}"
    job_id = f"job-{uuid.uuid4().hex[:10]}"
    paths = mk_run_dirs(run_id)

    # Persist URLs to manifest (optional for the agent, but useful)
    write_urls_manifest(paths["input"], body.input_urls)

    # Create job record in 'queued' then launch process
    status = RunStatus(job_id=job_id, run_id=run_id, status="queued", error=None, outputs=None)
    JOBS[job_id] = status

    try:
        proc = launch_worker(run_id, paths, body.input_urls)
        PIDS[job_id] = proc.pid
        JOBS[job_id] = RunStatus(job_id=job_id, run_id=run_id, status="running", error=None, outputs=None)
    except Exception as e:
        JOBS[job_id] = RunStatus(job_id=job_id, run_id=run_id, status="failed", error=str(e), outputs=None)

    return JOBS[job_id]


@app.get("/runs/{job_id}", response_model=RunStatus)
def runs_status(req: Request, job_id: str):
    if job_id not in JOBS:
        # keep old message to match your n8n checks
        raise HTTPException(status_code=404, detail="unknown job_id")

    current = JOBS[job_id]
    pid = PIDS.get(job_id)

    # If already terminal, just return with outputs attached
    if current.status in ("succeeded", "failed"):
        # Ensure outputs are present (idempotent)
        if current.outputs is None:
            JOBS[job_id].outputs = collect_outputs(req, current.run_id)
        return JOBS[job_id]

    # If running, check the process
    if current.status == "running" and pid:
        try:
            # poll process by pid
            proc_alive = True
            # Use /proc if available; else attempt a non-blocking wait
            ret = os.waitpid(pid, os.WNOHANG)
            if ret == (0, 0):
                proc_alive = True
            else:
                # Child finished; ret[1] has exit code in POSIX encoding
                exit_code = os.WEXITSTATUS(ret[1]) if os.WIFEXITED(ret[1]) else 1
                proc_alive = False
        except ChildProcessError:
            # Already reaped
            proc_alive = False
            exit_code = 0
        except Exception:
            # Fallback: try to check by collecting outputs and guessing
            proc_alive = False
            exit_code = 0

        if proc_alive:
            return current

        # Process finished; compute outputs and finalize status
        outputs = collect_outputs(req, current.run_id)
        JOBS[job_id].outputs = outputs

        # Decide success/failure by presence of CSV (and exit code if available)
        csv_ok = bool(outputs.get("csv_path"))
        # If stderr has content, include a readable message
        err_msg = None
        stderr_path = outputs.get("stderr_path")
        if stderr_path and os.path.isfile(stderr_path):
            try:
                size = os.path.getsize(stderr_path)
                if size > 0:
                    # keep it short
                    with open(stderr_path, "r", encoding="utf-8", errors="ignore") as f:
                        snippet = f.read(1000)
                    err_msg = f"agent stderr (first 1KB):\n{snippet}"
            except Exception:
                pass

        if csv_ok:
            JOBS[job_id] = RunStatus(job_id=job_id, run_id=current.run_id, status="succeeded", error=None, outputs=outputs)
        else:
            JOBS[job_id] = RunStatus(job_id=job_id, run_id=current.run_id, status="failed", error=err_msg or "no csv produced", outputs=outputs)

        return JOBS[job_id]

    # If queued but not started (rare), just return as-is
    return current


@app.get("/download")
def download(path: str = Query(..., description="Absolute path under /tmp to serve")):
    """
    Secure file download: only allows paths under /tmp to avoid leaking server files.
    """
    if not path:
        raise HTTPException(status_code=400, detail="path is required")

    # Basic safety: normalize and ensure under /tmp
    norm = os.path.realpath(path)
    if not norm.startswith(os.path.realpath(BASE_TMP) + os.sep):
        raise HTTPException(status_code=403, detail="forbidden path")

    if not os.path.isfile(norm):
        raise HTTPException(status_code=404, detail="file not found")

    # Let Starlette set content-type by filename; CSV/text will download/view fine
    return FileResponse(norm, filename=os.path.basename(norm))


# -------------
# If you want to run locally:
# uvicorn app:app --reload --port 10000
# -------------
