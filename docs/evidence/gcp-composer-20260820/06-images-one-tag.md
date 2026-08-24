# All six DAG images published on one derived tag

## `make build-images` — one derived tag exported to both build scripts
```
building every image on tag 868d958-dirty
image version: 868d958-dirty
published:
  europe-west1-docker.pkg.dev/mig-000001-1-dev/mig-dataflow/loader-app:868d958-dirty
  europe-west1-docker.pkg.dev/mig-000001-1-dev/mig-dataflow/recon-service:868d958-dirty
  europe-west1-docker.pkg.dev/mig-000001-1-dev/mig-dataflow/dataform-runner:868d958-dirty
image version: 868d958-dirty
✓ 3 Flex Templates published.
all images published on tag 868d958-dirty
pass it to terraform as -var=java_image_tag=868d958-dirty
```

## Artifact Registry, after the run
```
data-enrichment	868d958-dirty,latest
dataform-runner	868d958-dirty,latest
file-processor	868d958-dirty,latest
json-producer	868d958-dirty,latest
loader-app	868d958-dirty,latest
recon-service	868d958-dirty,latest
```

## The Composer environment pins that same tag
```
MIG_EXECUTION_MODE=dataflow
MIG_JAVA_IMAGE_TAG=868d958-dirty
```
