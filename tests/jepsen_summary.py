"""One line summarising a Jepsen run, from its log: the faults injected and the workload's counts.

    python3 tests/jepsen_summary.py jepsen/store/ryw-...-latest.log
"""

import re
import sys
from collections import Counter

log = open(sys.argv[1], encoding="utf-8", errors="replace").read()

faults = Counter(m.group(1) for m in re.finditer(r":nemesis\s+:info\s+:(pause|kill|partition|skew)\s+\[", log))


def first(key):
    m = re.search(r":" + re.escape(key) + r" (-?\d+)", log)
    return m.group(1) if m else None


parts = []
for key, label in [("increments", "increments"), ("reads-answered", "reads answered"), ("stale-reads", "stale"),
                   ("reads-not-yet", "not yet"), ("writes", "writes"), ("drift", "drift"),
                   ("deltas-folded", "deltas folded"), ("ok-count", "ok"), ("fail-count", "failed")]:
    v = first(key)
    if v is not None:
        parts.append(f"{v} {label}")
v = first("indeterminate") or first("info-count")
if v is not None:
    parts.append(f"{v} indeterminate")
anomalies = re.search(r":anomaly-types \(([^)]*)\)", log)
if anomalies:
    parts.append("anomalies: " + (anomalies.group(1) or "none"))
if faults:
    parts.append("faults: " + ", ".join(f"{n} {f}" for f, n in sorted(faults.items())))
print("; ".join(parts) or "no results (see the log)")
