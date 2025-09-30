import os
import shutil
import subprocess
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from fastapi.responses import FileResponse

app = FastAPI()

RUNS_BASE = "/tmp"

class RunRequest(BaseModel):
    run_id: str
    input_urls: List[str]

class RunStatus(BaseModel):
    job_id: str
    run_id: str
    status: str
    error: Optional[str] = None
    outputs: Optional[dict] = None

jobs = {}

@app.post("/runs_async")
async def start_run(req: RunRequest):
    job_id = "job-" + uuid.uuid4().hex[:10]
    run_dir = os.path.join(RUNS_BASE, req.run_id)
    os.makedirs(run_dir, exist_ok=True)
    input_dir = os.path.join(run_dir, "input")
    os.makedirs(input_dir, exist_ok=True)
    parsed_dir = os.path.join(run_dir, "parsed")
    output_dir = os.path.join(run_dir, "output")
    logs_dir = os.path.join(run_dir, "logs")
    for d in [parsed_dir, output_dir, logs_dir]:
        os.makedirs(d, exist_ok=True)

    # Download PDFs
    for url in req.input_urls:
        fname = os.path.basename(url.split("?")[0])
        dest = os.path.join(input_dir, fname)
        try:
            import requests
            r = requests.get(url)
            with open(dest, "wb") as f:
                f.write(r.content)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to download {url}: {e}")

    jobs[job_id] = {"status": "queued", "run_id": req.run_id}

    # Run the parser agent
    try:
        result = subprocess.run(
            [
                "python", "run_agent.py",
                "--run-id", req.run_id,
                "--input", input_dir,
                "--parsed", parsed_dir,
                "--output", output_dir,
                "--logs", logs_dir,
            ],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            jobs[job_id]["status"] = "succeeded"
        else:
            jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = result.stderr.strip() if result.returncode != 0 else None
from urllib.parse import quote

base = os.getenv("PUBLIC_BASE_URL", "https://fr-po-parser.onrender.com").rstrip("/")

def mk_url(abs_path: str) -> str:
    return f"{base}/download?path={quote(abs_path, safe='')}"

combined_path = os.path.join(output_dir, f"combined_{req.run_id}.csv")
stdout_path   = os.path.join(logs_dir, "agent.stdout.txt")
stderr_path   = os.path.join(logs_dir, "agent.stderr.txt")

# Collect debug/*.txt files if they exist
debug_dir = os.path.join(os.path.dirname(output_dir), "debug")
debug_urls = []
if os.path.exists(debug_dir):
    for fname in os.listdir(debug_dir):
        if fname.lower().endswith(".txt"):
            absf = os.path.join(debug_dir, fname)
            debug_urls.append(mk_url(absf))

jobs[job_id]["outputs"] = {
    "combined_csv": combined_path,
    "combined_csv_url": mk_url(combined_path),
    "stdout_url": mk_url(stdout_path),
    "stderr_url": mk_url(stderr_path),
    "debug_urls": debug_urls,
}
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)

    return {"job_id": job_id, "run_id": req.run_id, "status": jobs[job_id]["status"]}

@app.get("/runs/{job_id}")
async def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="unknown job_id")
    return jobs[job_id]

@app.get("/download")
async def download_file(path: str):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="file not found")
    return FileResponse(path)
