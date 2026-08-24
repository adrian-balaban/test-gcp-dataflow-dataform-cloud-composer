# Post-fix DAG retest, 2026-08-19 — blocked by SSD quota, not by the code

The five review fixes (ledger column rename, run-id guard, non-vacuous batch check,
json_producer purge, writer docstring) were re-tested on GCP. The DAG never got past
`file_processor`, and the reason is infrastructure:

```
FailedScheduling  0/5 nodes are available: 2 Insufficient cpu, 5 Insufficient memory
FailedScaleUp     Node scale up in zones europe-west1-c failed: GCE quota exceeded
NotTriggerScaleUp Pod didn't trigger scale-up: 19 in backoff after failed scale-up
```

`SSD_TOTAL_GB` was **500/500**. Composer 2's Autopilot cluster runs five nodes at 100 GB
each — exactly the default limit — so no sixth node could be created for a task pod.
Regional CPU was 10/200, and `gcloud compute instances list` showed **zero** instances,
because Autopilot's nodes live in a Google-managed tenant project while their disks count
against this project's quota.

Two attempts were made, the second after raising `startup_timeout_seconds` from 600 to 900
on the large-image tasks. Both timed out waiting for a node that could not exist.

## What this run did prove

Before the quota wall, the run exercised the parts that had failed on 08-18:

- The **tag mismatch was real and is fixed.** `build_java_images.sh` honoured
  `MIG_JAVA_IMAGE_TAG` but `build_templates.sh` did not, so the Java images were published
  as `:review-fixes` while the Beam images stayed on the git SHA — and the DAG launches both
  from a *single* variable. `file_processor` could not pull its image. Both scripts now take
  the override; all six images were verified present under `:review-fixes`.
- RBAC, Workload Identity, the Terraform-managed environment variables and the DAG deploy
  all worked on a freshly created environment.

## What remains unproven

The five fixes have **not** been exercised through the DAG. They are covered locally —
33 Python tests, 10 Java tests, all 9 acceptance criteria, and `verify-project2` — and the
same DAG path ran green end-to-end on 08-18 with the pre-fix code (`02-dag-task-states.md`).

The fix carrying the most GCP-path risk is the `target_written` → `extraction_written`
rename, because the DAG's `assert_run_balanced` reads that column. The DAG query was updated
in the same commit and the live table migrated with `ALTER TABLE … RENAME COLUMN`, but the
gate has not run against the renamed column on Composer.

**To close this gap:** request an `SSD_TOTAL_GB` increase for the region, then re-run the DAG
against the extract in `gs://<project>-landing/extraction/initial-20260818-165241/`.
