'''Generated with ChatGPT 5.2 prompt
scrape-faa-releasable-aircraft
Every day it creates a new commit that takes ReleasableAircraft zip from FAA takes Master.txt to make these files (it does this so that all files stay under 100mb). For every commit day I want to recombine all the files into one Master.txt again. It has data/commits since 2023.
scrape-faa-releasable-aircraft % ls
ACFTREF.txt     DOCINDEX.txt    MASTER-1.txt    MASTER-3.txt    MASTER-5.txt    MASTER-7.txt    MASTER-9.txt    RESERVED.txt
DEALER.txt      ENGINE.txt      MASTER-2.txt    MASTER-4.txt    MASTER-6.txt    MASTER-8.txt    README.md       ardata.pdf
'''
import subprocess, re
from pathlib import Path
from collections import OrderedDict

def run(*args: str) -> str:
    return subprocess.check_output(args, text=True).strip()

# Get commits that touched any MASTER-*.txt, oldest -> newest
log = run("git", "log", "--reverse", "--format=%H %cs", "--", ".")
# If you want to restrict to only commits that touched the master parts, use:
# log = run("git", "log", "--reverse", "--format=%H %cs", "--", "MASTER-1.txt")

lines = [ln for ln in log.splitlines() if ln.strip()]
if not lines:
    raise SystemExit("No commits found.")

# Map date -> last commit SHA on that date (Ordered by history)
date_to_sha = OrderedDict()
for ln in lines:
    sha, date = ln.split()
    # keep last SHA per day
    date_to_sha[date] = sha

out_root = Path("out_master_by_day")
out_root.mkdir(exist_ok=True)

master_re = re.compile(r"^MASTER-(\d+)\.txt$")

for date, sha in date_to_sha.items():
    # list files at this commit, filter MASTER-*.txt in repo root
    names = run("git", "ls-tree", "--name-only", sha).splitlines()
    parts = []
    for n in names:
        m = master_re.match(n)
        if m:
            parts.append((int(m.group(1)), n))
    parts.sort()

    if not parts:
        # no master parts in that commit/day; skip
        continue

    day_dir = out_root / date
    day_dir.mkdir(parents=True, exist_ok=True)
    out_path = day_dir / "Master.txt"

    with out_path.open("wb") as w:
        for _, fname in parts:
            data = subprocess.check_output(["git", "show", f"{sha}:{fname}"])
            w.write(data)
            if data and not data.endswith(b"\n"):
                w.write(b"\n")

    print(f"{date} {sha[:7]} -> {out_path} ({len(parts)} parts)")

print(f"\nDone. Output root: {out_root.resolve()}")
