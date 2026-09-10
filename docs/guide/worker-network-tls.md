# Deploying workers with mutual TLS

This guide describes the worker mTLS implementation in this branch. Use worker
and CLI binaries built from this revision; the examples do not assume that a
published image already includes it.

For N workers, provide N certificate/key pairs and one shared CA trust bundle.
Each worker uses its identity both when connecting and when accepting connections.
There are no certificates to create for individual pairs of workers. Once names,
trust, and routing are configured, any worker can establish an mTLS connection to
any other worker.

TLS 1.3 protects the worker network port, including its internal control and data
connections. The separate gRPC API and external source/sink connections need their
own transport protection. The [implementation README](../../nes-network/README.md)
describes the exact scope and remaining limitations.

## What the administrator provides

| Item | Requirement |
| --- | --- |
| Worker address | A stable, reachable DNS name and data port for each worker. Use exactly this name in its `data_address` and the frontend topology. |
| Worker certificate | PEM leaf certificate followed by any intermediate chain, with the worker DNS name in a subject alternative name (SAN), and both `serverAuth` and `clientAuth` extended key usages. An IP address requires an IP SAN instead. |
| Worker private key | Matching unencrypted PEM key, readable by that worker process. Each worker gets its own key. |
| CA trust bundle | Public PEM CA certificates approved for worker authentication, installed on every worker. Keep CA signing keys at the issuer. |
| Networking | Permit every worker to initiate TCP connections to every other worker's data port. Permit the controller to reach each worker's gRPC API. Allow DNS and keep clocks synchronized. |
| Operations | Certificate issuance, expiry monitoring, renewal, and worker restarts to load changed credentials. |

Use a CA dedicated to this deployment's trust boundary where possible. Any valid
client certificate accepted by `ca_file` can connect; NES does not bind that
certificate to a worker ID or apply per-worker authorization. A broad corporate
or public trust store would therefore broaden who can connect.

### On VMs or physical hosts

Have your PKI issue the certificates above. For `worker-1.example.internal`, place
its chain and key under `/etc/nes/identity/` and the shared public bundle under
`/etc/nes/trust/`. Restrict key access to the worker service account, for example
owner/group `nes:nes` and mode `0640`; parent directories must be traversable by it.
The public bundle is not secret, but only administrators should be able to change it.

```yaml
grpc: 0.0.0.0:8080
data_address: worker-1.example.internal:9090
worker:
  network:
    tls:
      enabled: true
      certificate_file: /etc/nes/identity/tls.crt
      private_key_file: /etc/nes/identity/tls.key
      ca_file: /etc/nes/trust/ca.crt
      handshake_timeout_ms: 10000
```

Start each worker with `nes-single-node-worker --configPath=/etc/nes/worker.yaml`
under its service account, with a writable working directory for logs and file
sinks. Repeat with that worker's address and identity. All communicating workers
must enable TLS; handshake failures never fall back to plaintext.

## Reusing an existing Kubernetes TLS setup

The integration point is the three mounted PEM files. NES does not discover a CA
from Kubernetes or load the operating system's certificate store. An existing
certificate agent or volume provider can supply these files if they meet the
requirements above; there is no built-in issuance or secret-provider API client.

| Existing setup | How to connect it to NES |
| --- | --- |
| Internal PKI / cert-manager issuer | Issue one worker certificate per address, mount its Secret, and mount a separately managed public CA bundle. The example below does this. |
| TLS at an ingress | [Ingress termination](https://kubernetes.io/docs/concepts/services-networking/ingress/#tls) covers connections passing through that ingress. Worker-to-worker TCP connections still need protection. Reuse the issuing infrastructure if it supports worker names and both certificate usages. |
| Service mesh mTLS | The mesh can protect worker TCP traffic when its policy covers every connection and rejects plaintext bypasses. NES can then run with TLS disabled inside that boundary, or carry its own mTLS through the mesh as opaque TCP. Validate port handling and health probes against the mesh configuration. |
| Kubernetes API CA (`kube-root-ca.crt` or the service-account CA file) | This is for Kubernetes endpoints. Obtain the workload CA bundle from your PKI administrator instead. |

Kubernetes documents the [separation between its API CA and workload CAs](https://kubernetes.io/docs/tasks/tls/managing-tls-in-a-cluster/#trusting-tls-in-a-cluster).
For a mesh, consult its [workload mTLS policy](https://istio.io/latest/docs/concepts/security/).
NES currently checks DNS/IP SANs; a certificate containing only a SPIFFE URI
identity cannot satisfy that check. Reusing the mesh's CA alone is insufficient
unless the issued worker certificates also meet NES's requirements.

### 1. Select an issuer and distribute trust

The example assumes an existing cert-manager `ClusterIssuer` named
`nes-workload-issuer`, authorized to issue private service DNS names with both
server and client usages. An ingress ACME issuer is not a substitute for that
policy. For a namespaced `Issuer`, change `issuerRef.kind` in the template and
ensure the issuer exists in the workers' namespace. External issuer types may
also require a different `issuerRef.group`.

Choose the public trust bundle with your PKI administrator. Keep it separate from
the worker Secret so it can contain both old and new CAs during rotation. Do not
point `ca_file` directly at cert-manager's renewing leaf Secret's `ca.crt` field;
it may change with issuance or be absent. See cert-manager's
[trust distribution guidance](https://cert-manager.io/docs/trust/).

Commands below run from the repository root in Bash and use `kubectl` and
`envsubst` (GNU gettext). Set the image to one you have built and pushed with this
implementation, preferably pinned by digest:

```bash
export NES_NAMESPACE=nes
export NES_CLUSTER_DOMAIN=cluster.local   # Use your cluster's actual DNS suffix.
export NES_WORKERS=3                     # At least two for the smoke query.
export NES_ISSUER=nes-workload-issuer
export NES_WORKER_IMAGE='registry.example.com/nes-worker:your-build'
kubectl create namespace "$NES_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
```

If the platform already distributes an approved trust ConfigMap, use its name and
key in the template's `trust` volume. Otherwise, create one from the public bundle
provided by your PKI administrator:

```bash
kubectl -n "$NES_NAMESPACE" create configmap nes-worker-ca \
  --from-file=ca.crt=/path/to/approved-worker-ca-bundle.pem \
  --dry-run=client -o yaml | kubectl apply -f -
```

Alternatively, if trust-manager is installed, let it own this ConfigMap. Put the
approved public CA bundle in ConfigMap `nes-worker-ca-source`, key `ca.crt`, in
trust-manager's **configured trust namespace**. Apply the following cluster-scoped
Bundle and label the target namespace:

```bash
kubectl label namespace "$NES_NAMESPACE" nes-worker-trust=enabled --overwrite
kubectl apply -f - <<'YAML'
apiVersion: trust.cert-manager.io/v1alpha1
kind: Bundle
metadata:
  name: nes-worker-ca
spec:
  sources:
    - configMap:
        name: nes-worker-ca-source
        key: ca.crt
  target:
    configMap:
      key: ca.crt
    namespaceSelector:
      matchLabels:
        nes-worker-trust: "enabled"
YAML
kubectl wait --for=condition=Synced bundle/nes-worker-ca --timeout=120s
```

This produces ConfigMap `nes-worker-ca` in the selected namespaces. Choose one
owner for the target ConfigMap; do not combine manual updates with trust-manager
reconciliation. See the [Bundle source and target rules](https://cert-manager.io/docs/trust/trust-manager/).
The worker's `ca_file` will be `/etc/nes/trust/ca.crt` in either case.

### 2. Render and start N workers

The [worker template](examples/worker-network-tls/kubernetes-worker.yaml.in)
creates a Certificate, configuration ConfigMap, Service, and single-replica
Deployment for one worker. Render it once per worker:

```bash
mkdir -p /tmp/nes-mtls-manifests
for i in $(seq 1 "$NES_WORKERS"); do
  export NES_WORKER="worker-$i"
  envsubst '${NES_WORKER} ${NES_NAMESPACE} ${NES_CLUSTER_DOMAIN} ${NES_ISSUER} ${NES_WORKER_IMAGE}' \
    < docs/guide/examples/worker-network-tls/kubernetes-worker.yaml.in \
    > "/tmp/nes-mtls-manifests/$NES_WORKER.yaml"
done
```

Each Service selects exactly one worker identity. For example,
`worker-1.nes.svc.cluster.local` appears in both the certificate SAN and
`data_address`. The pod can be replaced without changing that address. This uses
[normal Kubernetes Service DNS](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#services).
The current NES listener binds IPv4, so the example explicitly uses IPv4 Services
and requires IPv4-capable pod networking.

Review the rendered manifests and adapt these deployment choices:

- Keep one replica per worker Deployment. To add a worker, render a new identity
  and Service. Scaling one of these Deployments to N would route an individual
  worker address to multiple independent processes. `Recreate` avoids overlap
  during Deployment updates; it does interrupt that worker's queries.
- UID/GID `10001` must suit your image. `fsGroup` lets the process read the mounted
  key and write its working volume. Mount only that worker's identity Secret.
- The 256 MiB NES buffer budget is not a container memory limit. Set Kubernetes
  resource requests/limits and NES thread/buffer settings for your workload.
- `emptyDir` holds logs and smoke-query output and is lost when the pod is
  replaced. Use suitable persistent storage for outputs that must survive it.
- Adapt network policies and firewalls: all worker pods need TCP 9090 to all
  peers, DNS access, and the controller needs TCP 8080 to every worker. Keep the
  gRPC API within an appropriately protected network or mesh; this change does
  not add gRPC TLS. The example does not expose either port outside the cluster.

The Certificate requests ECDSA P-256 keys and both authentication usages explicitly.
cert-manager stores the identity in `tls.crt` and `tls.key`; the issuer must honor
the requested usages. See the [Certificate API guidance](https://cert-manager.io/docs/usage/certificate/).

Apply only the files for the intended worker set, then wait for issuance and startup:

```bash
for i in $(seq 1 "$NES_WORKERS"); do
  kubectl apply -f "/tmp/nes-mtls-manifests/worker-$i.yaml"
done
for i in $(seq 1 "$NES_WORKERS"); do
  kubectl -n "$NES_NAMESPACE" wait --for=condition=Ready "certificate/worker-$i" --timeout=180s
  kubectl -n "$NES_NAMESPACE" rollout status "deployment/worker-$i" --timeout=180s
done
```

The probes use NES's gRPC health endpoint; they establish process readiness, not
successful mTLS communication between workers. Kubernetes documents
[gRPC probe behavior](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-grpc-liveness-probe).

### 3. Register addresses and verify a distributed query

TLS does not discover workers or create the query topology. Register each worker's
gRPC address and matching data address in your frontend. The `downstream` entries
describe the query placement graph; allowing all pairs through the network does
not require a fully connected or cyclic placement graph. See
[distributed deployment](../website/content/reference/Distributed.md).

The [smoke topology](examples/worker-network-tls/smoke-topology.yaml.in) registers
two distinct workers from the N-worker deployment, pins a generator to the first
and a file sink to the last, and transfers values 0 through 9999:

```bash
export NES_SOURCE="worker-1.$NES_NAMESPACE.svc.$NES_CLUSTER_DOMAIN"
export NES_SINK="worker-$NES_WORKERS.$NES_NAMESPACE.svc.$NES_CLUSTER_DOMAIN"
envsubst '${NES_SOURCE} ${NES_SINK}' \
  < docs/guide/examples/worker-network-tls/smoke-topology.yaml.in \
  > /tmp/nes-mtls-smoke.yaml
```

Copy this topology to your existing controller environment inside the cluster
(or one with equivalent DNS/routing), and run:

```bash
nes-cli -t /tmp/nes-mtls-smoke.yaml dump
nes-cli -t /tmp/nes-mtls-smoke.yaml start
nes-cli -t /tmp/nes-mtls-smoke.yaml status QUERY_ID_FROM_START
```

Use the same controller state directory for `start` and `status`. Wait until both
worker query fragments report `Stopped`; a successful `start` alone is not proof
of data transfer. Then, from the administrator's shell, verify every output value:

```bash
kubectl -n "$NES_NAMESPACE" exec "deployment/worker-$NES_WORKERS" -c worker -- \
  cat /var/lib/nes/mtls-smoke.csv > /tmp/nes-mtls-smoke.csv
seq 0 9999 > /tmp/nes-mtls-expected.csv
tail -n +2 /tmp/nes-mtls-smoke.csv | LC_ALL=C sort -n > /tmp/nes-mtls-actual.csv
diff -u /tmp/nes-mtls-expected.csv /tmp/nes-mtls-actual.csv
```

The first CSV line is a schema header. Empty `diff` output with exit status zero
confirms that all rows arrived. Repeat sequentially with other distinct source
and sink workers, including reversed pairs, to validate the required communication
paths. Select the corresponding sink Deployment when reading the output. Do not
run these checks concurrently against the same sink file.

For automated Docker coverage of trusted/untrusted peers and abrupt termination
in either direction, see the [mTLS Bats suite](../../nes-frontend/apps/cli/tests/mtls.bats)
and its [run instructions](../../nes-network/README.md#validation-and-remaining-scope).
Those tests do not validate your Kubernetes issuer, policy, or manifests.

## Renewal, replacement, and CA rotation

**NES loads identities and trust at startup.** cert-manager can renew Secrets, but
NES does not reload them. Arrange a worker restart after certificate/key or trust
bundle changes, before the certificates currently loaded in the process expire.
Use your existing rollout automation or perform a coordinated restart, for example:

```bash
kubectl -n "$NES_NAMESPACE" rollout restart deployment/worker-1
kubectl -n "$NES_NAMESPACE" rollout status deployment/worker-1 --timeout=180s
```

Directory mounts allow Kubernetes to project Secret updates; `subPath` mounts
do not receive them. Even with updated files visible, NES still needs the restart.
See [Secret volume updates](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod).
Coordinate restarts with queries; recovery of an interrupted query is not guaranteed.

For CA rollover:

1. Distribute a bundle containing both old and new public CAs to every worker.
2. Restart every worker and confirm it loaded that bundle before changing identities.
3. Issue replacement worker certificates under the new CA, restart their workers,
   and verify distributed queries. During this phase both CAs remain trusted.
4. After all old identities have been retired, remove the old CA from the bundle
   and restart every worker again.

Adding a worker requires only a new identity/address, the shared bundle, routing,
and topology registration. Removing it from the topology does not invalidate its
certificate. This implementation has no configured CRL/OCSP revocation checking;
if a key is compromised, removing its Kubernetes Secret does not revoke copies.
Plan certificate lifetimes and incident response with that limitation in mind.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Worker cannot start | All three paths exist and are readable; PEM chain/key match; TLS is enabled; `data_address` is set. A missing data address does not start worker network services. |
| `UnknownIssuer` | Both workers trust the issuer's CA; the peer sends any intermediate chain; every process has restarted after trust changes. |
| Certificate name failure | The exact hostname in the topology's `data_address` is a DNS SAN. Using a short name or an IP instead of the certified FQDN changes what is verified. |
| Certificate usage/expiry failure | The issued leaf has both client and server authentication usages, is within its validity period, and clocks agree. Inspect the issued certificate, not only its requested fields. |
| Handshake timeout | DNS, Service endpoints, TCP 9090 policies, mesh handling, and peer TLS mode. The outgoing timeout includes DNS, TCP establishment, and TLS. |
| Query stays pending while pods are Ready | Read worker logs for TLS failures and verify actual row transfer. Health probes do not check peer trust, and the current retry policy also retries certificate failures. |

Inspect a public leaf certificate without retrieving its private key:

```bash
kubectl -n "$NES_NAMESPACE" get secret worker-1-tls \
  -o jsonpath='{.data.tls\.crt}' | base64 --decode \
  | openssl x509 -noout -subject -issuer -dates -ext subjectAltName,extendedKeyUsage
```
