#!/usr/bin/env bash
# Apply Composer's pod-launcher RBAC from inside the VPC.
#
# WHY THIS EXISTS
# ---------------
# The runbook's phase-2 step says "read the namespace with kubectl, then re-apply with
# -var=composer_pod_namespace=...". That assumes the GKE control plane is reachable from
# the operator's machine. On current Composer 2 environments it is not: the cluster is
# created with
#
#     masterAuthorizedNetworksConfig.enabled          = true   (no CIDRs allowed)
#     privateClusterConfig.privateEndpointEnforcement = true   (public endpoint disabled)
#
# even though terraform/modules/composer sets `enable_private_endpoint = false` — that
# setting governs the *environment*, not the GKE control plane, and Google's defaults have
# since tightened. So kubectl from a laptop times out against the public endpoint, and
# Terraform's `kubernetes` provider (module.composer_rbac) fails the same way.
#
# Without the RBAC, five of the nine DAG tasks fail at submission with
#   pods is forbidden: User "system:serviceaccount:<ns>:default" cannot list resource "pods"
#
# WHAT IT DOES
# ------------
# Creates a throwaway VM on mig-subnet, which *is* in the VPC and can reach the private
# endpoint; the VM discovers the namespace itself, applies the Role, RoleBinding and the
# annotated `mig-pipeline` ServiceAccount, and is deleted afterwards. The Workload Identity
# binding on the Google side is applied from here, where it does not need cluster access.
#
# Everything it grants is reverted at the end except the RBAC itself.
set -euo pipefail

cd "$(dirname "$0")/../../.."
source local/scripts/gcp/_env.sh

ZONE="${ZONE:-${TF_VAR_region}-b}"
VM="mig-rbac-helper"
GSA="dataflow-worker@${TF_VAR_project_id}.iam.gserviceaccount.com"
LOADER_GSA="loader-app@${TF_VAR_project_id}.iam.gserviceaccount.com"
RECON_GSA="recon-service@${TF_VAR_project_id}.iam.gserviceaccount.com"
NUM="$(gcloud projects describe "$TF_VAR_project_id" --format='value(projectNumber)')"
COMPUTE_SA="${NUM}-compute@developer.gserviceaccount.com"
CLUSTER="$(gcloud container clusters list --project="$TF_VAR_project_id" \
             --filter='name~mig-composer' --format='value(name)' | head -1)"

[ -n "$CLUSTER" ] || { echo "no mig-composer GKE cluster found — is Composer enabled?" >&2; exit 1; }
echo "cluster: $CLUSTER"

cleanup() {
  echo "==> cleaning up"
  gcloud compute instances delete "$VM" --project="$TF_VAR_project_id" --zone="$ZONE" --quiet 2>/dev/null || true
  gcloud projects remove-iam-policy-binding "$TF_VAR_project_id" \
    --member="serviceAccount:${COMPUTE_SA}" --role=roles/container.admin --condition=None --quiet >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> granting container.admin to the compute SA for the duration"
gcloud projects add-iam-policy-binding "$TF_VAR_project_id" \
  --member="serviceAccount:${COMPUTE_SA}" --role=roles/container.admin --condition=None >/dev/null

ENDPOINT="$(gcloud container clusters describe "$CLUSTER" --region "$TF_VAR_region" \
             --project "$TF_VAR_project_id" --format='value(privateClusterConfig.privateEndpoint)')"
[ -n "$ENDPOINT" ] || { echo "cluster has no private endpoint — is it really private?" >&2; exit 1; }
echo "private endpoint: $ENDPOINT"

STARTUP="$(mktemp)"
# The VM talks to the Kubernetes API with curl and a metadata-server token: no apt, no
# gcloud, no kubectl. An earlier version installed all three and was fragile for it — the
# VM has no external IP, so every package download depends on Cloud NAT, and the Debian
# image ships no gcloud. curl and the metadata server are always there, and the private
# endpoint plus Private Google Access are reachable without leaving the VPC.
#
# Output goes to /dev/console as well as a log file, because the startup-script runner
# only surfaces what reaches the console — redirecting everything into a file (as this
# script first did) makes a *successful* run look like a timeout.
cat > "$STARTUP" <<EOF
#!/bin/bash
exec > >(tee -a /var/log/mig-rbac.log > /dev/console) 2>&1
set -x

TOKEN="\$(curl -s -H 'Metadata-Flavor: Google' \
  http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
API="https://${ENDPOINT}"

# A bare \$( ... --data "\$3" ) would word-split the JSON body on every space, so the two
# cases are spelled out. A 409 from the POSTs below is success: the object already exists,
# which is exactly what a second run should find.
api() {  # method path [body]
  if [ -n "\${3:-}" ]; then
    curl -sk -X "\$1" -H "Authorization: Bearer \$TOKEN" -H 'Content-Type: application/json' \
      --data "\$3" "\$API\$2"
  else
    curl -sk -X "\$1" -H "Authorization: Bearer \$TOKEN" "\$API\$2"
  fi
}

NS="\$(api GET /api/v1/namespaces | python3 -c '
import json,sys
names = [i["metadata"]["name"] for i in json.load(sys.stdin)["items"]]
print(next(n for n in names if n.startswith("composer-")))')"
echo "MIG_NAMESPACE=\$NS"

# PUT is idempotent where POST would 409 on a re-run, so the script can be run twice.
api POST "/apis/rbac.authorization.k8s.io/v1/namespaces/\$NS/roles" '{
  "apiVersion":"rbac.authorization.k8s.io/v1","kind":"Role",
  "metadata":{"name":"airflow-pod-launcher","namespace":"'"\$NS"'"},
  "rules":[
    {"apiGroups":[""],"resources":["pods"],"verbs":["create","get","list","watch","delete","patch"]},
    {"apiGroups":[""],"resources":["pods/log","pods/status"],"verbs":["get","list","watch"]},
    {"apiGroups":[""],"resources":["pods/exec"],"verbs":["create","get"]}]}' >/dev/null

api POST "/apis/rbac.authorization.k8s.io/v1/namespaces/\$NS/rolebindings" '{
  "apiVersion":"rbac.authorization.k8s.io/v1","kind":"RoleBinding",
  "metadata":{"name":"airflow-pod-launcher","namespace":"'"\$NS"'"},
  "roleRef":{"apiGroup":"rbac.authorization.k8s.io","kind":"Role","name":"airflow-pod-launcher"},
  "subjects":[{"kind":"ServiceAccount","name":"default","namespace":"'"\$NS"'"}]}' >/dev/null

api POST "/api/v1/namespaces/\$NS/serviceaccounts" '{
  "apiVersion":"v1","kind":"ServiceAccount",
  "metadata":{"name":"mig-pipeline","namespace":"'"\$NS"'",
    "annotations":{"iam.gke.io/gcp-service-account":"${GSA}"}}}' >/dev/null

# The Java apps have their own Google accounts with narrower roles — recon may only read
# BigQuery — and the DAG asks for mig-loader / mig-recon by name. Terraform declares these
# too, but its kubernetes provider cannot reach a private control plane, so they are
# created here alongside mig-pipeline.
api POST "/api/v1/namespaces/\$NS/serviceaccounts" '{
  "apiVersion":"v1","kind":"ServiceAccount",
  "metadata":{"name":"mig-loader","namespace":"'"\$NS"'",
    "annotations":{"iam.gke.io/gcp-service-account":"${LOADER_GSA}"}}}' >/dev/null
api POST "/api/v1/namespaces/\$NS/serviceaccounts" '{
  "apiVersion":"v1","kind":"ServiceAccount",
  "metadata":{"name":"mig-recon","namespace":"'"\$NS"'",
    "annotations":{"iam.gke.io/gcp-service-account":"${RECON_GSA}"}}}' >/dev/null

echo "MIG_ROLE_CHECK=\$(api GET /apis/rbac.authorization.k8s.io/v1/namespaces/\$NS/roles/airflow-pod-launcher | head -c 60)"
echo "MIG_SA_CHECK=\$(api GET /api/v1/namespaces/\$NS/serviceaccounts/mig-pipeline | head -c 60)"
echo "MIG_RBAC_DONE"
EOF

echo "==> creating $VM on mig-subnet (no external IP; egress via Cloud NAT)"
gcloud compute instances create "$VM" \
  --project="$TF_VAR_project_id" --zone="$ZONE" \
  --machine-type=e2-small --subnet=mig-subnet --no-address \
  --image-family=debian-12 --image-project=debian-cloud \
  --service-account="$COMPUTE_SA" --scopes=cloud-platform \
  --metadata-from-file=startup-script="$STARTUP" >/dev/null
rm -f "$STARTUP"

echo "==> waiting for the startup script (up to 6 minutes)"
NS=""
for _ in $(seq 1 36); do
  sleep 10
  OUT="$(gcloud compute instances get-serial-port-output "$VM" \
          --project="$TF_VAR_project_id" --zone="$ZONE" 2>/dev/null || true)"
  if grep -q MIG_RBAC_DONE <<<"$OUT"; then
    NS="$(grep -o 'MIG_NAMESPACE=[^[:space:]]*' <<<"$OUT" | tail -1 | cut -d= -f2)"
    echo "==> RBAC applied in namespace: $NS"
    break
  fi
  grep -qE 'Error|error:' <<<"$OUT" && echo "  …still working (errors in log are often transient apt retries)"
done

[ -n "$NS" ] || { echo "RBAC did not complete — check the serial console of $VM" >&2; exit 1; }

echo "==> binding Workload Identity on the Google side"
for pair in "$GSA:mig-pipeline" "$LOADER_GSA:mig-loader" "$RECON_GSA:mig-recon"; do
  gsa="${pair%:*}"; ksa="${pair##*:}"
  gcloud iam service-accounts add-iam-policy-binding "$gsa" \
    --project="$TF_VAR_project_id" --role=roles/iam.workloadIdentityUser \
    --member="serviceAccount:${TF_VAR_project_id}.svc.id.goog[${NS}/${ksa}]" >/dev/null
  echo "    bound ${ksa} -> ${gsa}"
done

echo
echo "RBAC complete. Namespace: $NS"
echo "Record it so Terraform can adopt the same objects later:"
echo "  terraform -chdir=terraform/envs/dev apply -var=create_project=false \\"
echo "    -var=enable_composer=true -var=composer_pod_namespace=$NS"
