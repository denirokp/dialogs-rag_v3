# -*- coding: utf-8 -*-
import subprocess, sys

steps = [
    [sys.executable, "analyze_dialogs_advanced.py", "--model", "gpt-4o-mini", "--whole_max", "8000", "--window_tokens", "1800"],
    [sys.executable, "consolidate_and_summarize.py"],
]

for cmd in steps:
    print("[run]", " ".join(cmd))
    rc = subprocess.call(cmd)
    if rc != 0:
        sys.exit(rc)
print("[ok] pipeline complete")