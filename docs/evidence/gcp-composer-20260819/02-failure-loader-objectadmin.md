# Failure — loader_app under its own identity, and the fix

Finding 5 gave the loader and recon tasks the Google service accounts whose roles
were written for them, instead of everything running as dataflow-worker. The very
first run under those identities failed, which is the point: the tailored roles had
never been exercised, so nobody had noticed one was too tight.

## The error
```
[base]         "message": "loader-app@mig-000001-1-dev.iam.gserviceaccount.com does not have storage.objects.delete access to the Google Cloud Storage object.",
[base]     "message": "loader-app@mig-000001-1-dev.iam.gserviceaccount.com does not have storage.objects.delete access to the Google Cloud Storage object.",
```

## Why
The loader writes its own `.RPT`/`.CHS`/`.ERR` into the recon bucket, and re-running
the same run id **overwrites** them. GCS implements an overwrite as create + delete,
so `roles/storage.objectCreator` is not enough — it permits the create and denies the
delete. `dataflow-worker` already had `objectAdmin`, which is why this never surfaced
while every pod borrowed that identity.

## Fix
`terraform/modules/iam` grants loader-app `roles/storage.objectAdmin` on the recon
bucket, matching the comment already sitting above dataflow-worker for the same
class of mistake. After the change the cleared tasks succeeded on the next try:
```
mig_000001_1_migration | 2026-08-19T15:13:07+00:00 | reconciliation       | success | 2026-08-19T15:45:18.640195+00:00 | 2026-08-19T15:45:40.797327+00:00
mig_000001_1_migration | 2026-08-19T15:13:07+00:00 | loader_app           | success | 2026-08-19T15:44:53.449563+00:00 | 2026-08-19T15:45:15.161086+00:00
mig_000001_1_migration | 2026-08-19T15:13:07+00:00 | assert_run_balanced  | success | 2026-08-19T15:45:44.315043+00:00 | 2026-08-19T15:45:49.899454+00:00
```
