# Jepsen: isomer 0.24.0

Run 2026-10-21 08:54 UTC by `tests/jepsen.sh report` against the three-node compose cluster
(a coordinator and two workers), 120 seconds per run, 10 clients, a fault every 10 seconds
healed 10 seconds later. What each workload checks is in `jepsen/src/isomerdb/jepsen.clj`.

| Workload | Faults | On | Verdict | Summary |
|---|---|---|---|---|
| append | pause, kill, partition | worker1, worker2 | valid | 3220 ok; 113 failed; 532 indeterminate; faults: 3 kill, 2 partition, 1 pause |
| ryw | pause, kill, partition | worker1, worker2 | valid | 434 increments; 381 reads answered; 0 stale; 45 not yet; 0 drift; 435 ok; 0 failed; 14 indeterminate; faults: 4 kill, 2 pause |
| mongo | pause, kill, partition | worker1, worker2 | valid | 455 increments; 400 reads answered; 0 stale; 55 not yet; 0 drift; 456 ok; 0 failed; 11 indeterminate; faults: 3 kill, 2 partition, 1 pause |
| incremental | pause, kill, partition | worker1, worker2 | valid | 3231 writes; 0 drift; 60505 deltas folded; 3232 ok; 0 failed; 340 indeterminate; faults: 3 kill, 2 partition, 1 pause |
| subqueries | pause, kill, partition | worker1, worker2 | valid | 3207 writes; 0 drift; 24022 deltas folded; 3208 ok; 4 failed; 746 indeterminate; faults: 2 kill, 2 partition, 2 pause |
| append | pause, kill, partition | db | valid | 2067 ok; 1047 failed; 29 indeterminate; faults: 2 kill, 3 partition, 1 pause |
| ryw | pause, kill, partition | db | valid | 319 increments; 284 reads answered; 0 stale; 25 not yet; 0 drift; 320 ok; 414 failed; 5 indeterminate; faults: 3 partition, 3 pause |
| mongo | pause, kill, partition | db | valid | 355 increments; 306 reads answered; 0 stale; 30 not yet; 0 drift; 356 ok; 77 failed; 10 indeterminate; faults: 1 kill, 3 partition, 2 pause |
| incremental | pause, kill, partition | db | valid | 2464 writes; 0 drift; 49288 deltas folded; 2465 ok; 137 failed; 30 indeterminate; faults: 2 kill, 3 partition, 1 pause |
| subqueries | pause, kill, partition | db | valid | 2624 writes; 0 drift; 23007 deltas folded; 2625 ok; 62 failed; 20 indeterminate; faults: 1 kill, 2 partition, 3 pause |
| append | skew | db, worker1, worker2 | valid | 5521 ok; 11 failed; 0 indeterminate; faults: 6 skew |
| ryw | skew | db, worker1, worker2 | valid | 515 increments; 505 reads answered; 0 stale; 8 not yet; 0 drift; 516 ok; 0 failed; 0 indeterminate; faults: 6 skew |
| mongo | skew | db, worker1, worker2 | valid | 442 increments; 368 reads answered; 0 stale; 64 not yet; 0 drift; 443 ok; 0 failed; 0 indeterminate; faults: 5 skew |
| incremental | skew | db, worker1, worker2 | valid | 5574 writes; 0 drift; 127071 deltas folded; 5575 ok; 0 failed; 0 indeterminate; faults: 6 skew |
| subqueries | skew | db, worker1, worker2 | valid | 5884 writes; 0 drift; 50431 deltas folded; 5885 ok; 0 failed; 0 indeterminate; faults: 6 skew |

Full histories, Elle's explanations and timelines are not kept in the repository; rerun to
reproduce (`jepsen/store/`).
