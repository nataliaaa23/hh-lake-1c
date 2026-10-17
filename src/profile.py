import csv
import os
import subprocess
import sys
import time

method, run = sys.argv[1], sys.argv[2]

commands = {
    "mapreduce": ["python", "src/mapreduce.py", "--no-upload"],
    "duckdb": ["python", "src/duckdb_skills.py"],
    "mapreduce_opt": ["python", "src/mapreduce_opt.py", "--no-upload"],
}


def peak_mb():
    for path in ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
        try:
            with open(path) as f:
                return int(f.read()) / 1024 / 1024
        except OSError:
            continue
    return -1


t0 = time.time()
proc = subprocess.run(commands[method], stdout=subprocess.DEVNULL)
elapsed = time.time() - t0

os.makedirs("results/profile", exist_ok=True)
path = "results/profile/runs.csv"
new = not os.path.exists(path)
with open(path, "a", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    if new:
        w.writerow(["method", "run", "seconds", "peak_mb", "exit_code"])
    w.writerow([method, run, round(elapsed, 1), round(peak_mb(), 1), proc.returncode])
print(method, run, round(elapsed, 1), round(peak_mb(), 1), proc.returncode)
