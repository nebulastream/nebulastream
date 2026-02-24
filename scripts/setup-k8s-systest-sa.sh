#!/usr/bin/env bash
# ==============================================================================
# setup-k8s-systest-sa.sh
#
# Creates a Kubernetes ServiceAccount with permissions to manage NebulaStream
# custom resources, and prints a bearer token suitable for the systest runner.
#
# Usage:
#   ./scripts/setup-k8s-systest-sa.sh
#
# After running, set the printed environment variables in your CLion Run
# Configuration (or shell) before running the systest:
#
#   export NES_K8S_ENABLED=1
#   export NES_K8S_BEARER_TOKEN=<token>
#   export NES_K8S_API_URL=https://$(minikube ip):8443
#
# When using CLion's Docker toolchain, also add --network=host to the Docker
# container run options so the container can reach minikube's API server.
# ==============================================================================

set -euo pipefail

NAMESPACE="${NES_K8S_NAMESPACE:-default}"
SA_NAME="nes-systest-sa"
SECRET_NAME="nes-systest-sa-token"
ROLE_NAME="nes-systest-role"
BINDING_NAME="nes-systest-rolebinding"

echo "==> Setting up ServiceAccount '${SA_NAME}' in namespace '${NAMESPACE}'..."

# --- ServiceAccount ---
kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SA_NAME}
  namespace: ${NAMESPACE}
EOF

# --- ClusterRole with minimal permissions for NES CRDs ---
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ${ROLE_NAME}
rules:
  - apiGroups: ["nebulastream.com"]
    resources: ["nes-topologies", "nes-queries"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "patch", "update"]
EOF

# --- ClusterRoleBinding ---
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${BINDING_NAME}
subjects:
  - kind: ServiceAccount
    name: ${SA_NAME}
    namespace: ${NAMESPACE}
roleRef:
  kind: ClusterRole
  name: ${ROLE_NAME}
  apiGroup: rbac.authorization.k8s.io
EOF

# --- Long-lived Secret (token) bound to the ServiceAccount ---
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: ${SECRET_NAME}
  namespace: ${NAMESPACE}
  annotations:
    kubernetes.io/service-account.name: ${SA_NAME}
type: kubernetes.io/service-account-token
EOF

# Wait for the token to be populated by the token controller
echo "==> Waiting for token to be populated..."
for i in $(seq 1 30); do
    TOKEN=$(kubectl get secret "${SECRET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.data.token}' 2>/dev/null || true)
    if [[ -n "${TOKEN}" ]]; then
        break
    fi
    sleep 1
done

if [[ -z "${TOKEN:-}" ]]; then
    echo "ERROR: Token was not populated after 30 seconds." >&2
    exit 1
fi

DECODED_TOKEN=$(echo "${TOKEN}" | base64 -d)
API_URL="https://$(minikube ip):8443"

echo ""
echo "==> Done! Add these environment variables to your CLion Run Configuration or shell:"
echo ""
echo "  export NES_K8S_ENABLED=1"
echo "  export NES_K8S_BEARER_TOKEN=${DECODED_TOKEN}"
echo "  export NES_K8S_API_URL=${API_URL}"
echo ""
echo "==> If using CLion Docker toolchain, also add to container run options:"
echo "     --network=host"
echo ""

