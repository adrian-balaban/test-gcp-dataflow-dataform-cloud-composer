# Infrastructure created for this test — 2026-08-18, project mig-000001-1-dev

The environment had been destroyed at the end of the 2026-08-05 session, including the
Terraform state bucket, so this was a bootstrap from an empty project, not a re-apply.

## Bootstrap + base infrastructure
```
/tmp/claude-1000/-home-adrianb---ai-assisted-github-test-gcp-dataflow-dataform-cloud-composer/59157a5a-84eb-4e46-b4ff-59ab38234a61/scratchpad/tf-apply.log:Apply complete! Resources: 57 added, 0 changed, 0 destroyed.
/tmp/claude-1000/-home-adrianb---ai-assisted-github-test-gcp-dataflow-dataform-cloud-composer/59157a5a-84eb-4e46-b4ff-59ab38234a61/scratchpad/tf-bootstrap.log:Apply complete! Resources: 15 added, 0 changed, 0 destroyed.
```

## Composer + Kafka (the billable pair, default off)
```
Apply complete! Resources: 5 added, 1 changed, 0 destroyed.
```

## Target System stand-in on Cloud Run — the Load lane had no target on GCP before this
```
NAME: target-system-mock
URL: https://target-system-mock-gztuytw7na-ew.a.run.app
```

## Composer environment variables — declared in Terraform, not set by hand
```
(captured while the environment was RUNNING, before teardown)
DATAFLOW_SERVICE_ACCOUNT=dataflow-worker@mig-000001-1-dev.iam.gserviceaccount.com
DATAFLOW_SUBNETWORK=regions/europe-west1/subnetworks/mig-subnet
KAFKA_BOOTSTRAP=bootstrap.mig-kafka.europe-west1.managedkafka.mig-000001-1-dev.cloud.goog:9092
MIG_JAVA_IMAGE_TAG=cf5f615-dirty
MIG_TARGET_SYSTEM_URL=https://target-system-mock-gztuytw7na-ew.a.run.app
```
