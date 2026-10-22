# Jepsen: isomer 0.21.0

Run 2026-10-13 21:19 UTC by `tests/jepsen.sh report` against the three-node compose cluster
(a coordinator and two workers), 120 seconds per run, 10 clients, a fault every 10 seconds
healed 10 seconds later. What each workload checks is in `jepsen/src/isomerdb/jepsen.clj`.

| Workload | Faults | On | Verdict | Summary |
|---|---|---|---|---|
| append | pause, kill, partition | worker1, worker2 | valid | 3354 ok; 127 failed; 628 indeterminate; faults: 3 kill, 2 partition, 1 pause |
| ryw | pause, kill, partition | worker1, worker2 | valid | 529 increments; 480 reads answered; 0 stale; 44 not yet; 0 drift; 530 ok; 0 failed; 4 indeterminate; faults: 1 kill, 2 partition, 3 pause |
| mongo | pause, kill, partition | worker1, worker2 | valid | 397 increments; 358 reads answered; 0 stale; 29 not yet; 0 drift; 398 ok; 1 failed; 5 indeterminate; faults: 2 kill, 1 partition, 3 pause |
| incremental | pause, kill, partition | worker1, worker2 | valid | 3239 writes; 0 drift; 61302 deltas folded; 3240 ok; 0 failed; 194 indeterminate; faults: 1 kill, 3 partition, 2 pause |
| append | pause, kill, partition | db | valid | 2380 ok; 52 failed; 17 indeterminate; faults: 1 kill, 3 partition, 2 pause |
| ryw | pause, kill, partition | db | valid | 305 increments; 251 reads answered; 0 stale; 19 not yet; 0 drift; 306 ok; 533 failed; 40 indeterminate; faults: 4 kill, 1 partition, 1 pause |
| mongo | pause, kill, partition | db | valid | 380 increments; 323 reads answered; 0 stale; 44 not yet; 0 drift; 381 ok; 30 failed; 17 indeterminate; faults: 1 kill, 3 partition, 2 pause |
| incremental | pause, kill, partition | db | valid | 2023 writes; 0 drift; 45437 deltas folded; 2024 ok; 1342 failed; 20 indeterminate; faults: 2 kill, 4 partition |
| append | skew | db, worker1, worker2 | valid | 5777 ok; 4 failed; 0 indeterminate; faults: 6 skew |
| ryw | skew | db, worker1, worker2 | valid | 511 increments; 481 reads answered; 0 stale; 20 not yet; 0 drift; 512 ok; 0 failed; 0 indeterminate; faults: 6 skew |
| mongo | skew | db, worker1, worker2 | valid | 323 increments; 240 reads answered; 0 stale; 69 not yet; 0 drift; 324 ok; 0 failed; 0 indeterminate; faults: 6 skew |
| incremental | skew | db, worker1, worker2 | valid | 5974 writes; 0 drift; 124188 deltas folded; 5975 ok; 0 failed; 0 indeterminate; faults: 6 skew |

Full histories, Elle's explanations and timelines are not kept in the repository; rerun to
reproduce (`jepsen/store/`).
