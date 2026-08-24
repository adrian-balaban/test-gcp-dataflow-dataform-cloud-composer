# Pod-based DAG run — 2026-08-05 11:01 (Europe/Bucharest)

Second real-GCP Composer run, after switching the three Beam tasks from
`DataflowStartFlexTemplateOperator` to `KubernetesPodOperator` (option C).

## What changed since the 07:41 run

| | 07:41 run | 11:01 run |
|---|---|---|
| Beam tasks | Flex Templates | `KubernetesPodOperator` pods |
| Images | 3 templates | all 5 at one tag `6f3854e` |
| DAG registered | yes | yes — `KubernetesPodOperator` imports fine on Airflow 2.9.3 |

## Progress made

- **The DAG parses and registers on Composer.** `airflow dags list` shows
  `mig_000001_1_migration`, unpaused. The rewritten DAG — with `beam_pipeline`,
  `java_app`, and no `MIG_EXECUTION_MODE` branching — is valid Airflow 2.9.
- **`wait_for_extract_flg` — success**, again. The GCS sensor found the `.FLG`.
- **The Flex Template lifecycle defect is gone.** No more
  `ValueError: Can not query metrics. Job id is unknown.` — running the pipeline as a
  process that owns its own Dataflow job is the right shape.

## The new blocker: Composer workers cannot create pods

```
pods is forbidden: User "system:serviceaccount:composer-2-9-7-airflow-2-9-3-ab8412f5:default"
cannot list resource "pods" in API group "" in the namespace "composer-2-9-7-airflow-2-9-3-ab8412f5"
```

Tried `namespace="default"` first — same 403, different namespace. So it is not the
namespace: **Composer 2 does not grant its Airflow workers RBAC to create arbitrary
pods**, in its own namespace or any other.

This is a real prerequisite that the DAG's `KubernetesPodOperator` usage depends on and
that nothing in `terraform/` currently creates. It applies equally to the Java
`loader_app` and `reconciliation` tasks — so **H5 is still unproven**, now for an
infrastructure reason rather than a pipeline one.

### What the fix looks like

A `Role` + `RoleBinding` on the Composer GKE cluster, granting
`system:serviceaccount:<composer-ns>:default` create/get/list/delete on `pods` and
`pods/log` in that namespace. Two honest options:

1. **Terraform** — a `kubernetes_role_binding` resource, which means adding the
   Kubernetes provider and wiring it to the Composer cluster's endpoint and CA. Correct
   and reproducible; it is the version that belongs in the repo.
2. **`kubectl apply`** — a one-liner, but out-of-band state that the next person will not
   know about. Fine to unblock a demo, wrong as the durable answer.

Blocked here on tooling: `kubectl` is installed but `gke-gcloud-auth-plugin` is not, so
the cluster cannot be reached from this machine without installing it first.

## Honest status

Three defect classes have been fixed and verified on real GCP (five Flex Template
defects, the impersonation grant, the Kafka bootstrap output). The Composer
orchestration path now fails at a **fourth, different layer**: GKE RBAC.

The data path remains proven end to end on real GCP through `run_pipeline.py`
(`docs/evidence/gcp-run-20260804/` — 40 documents, all 8 acceptance criteria green).
What is unproven is Composer *launching* the work, and the reason is now a missing
cluster-level permission rather than anything in the pipelines.
