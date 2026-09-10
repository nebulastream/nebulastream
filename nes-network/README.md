# Worker network mTLS sketch

The worker transport supports optional mutual TLS (mTLS) 1.3 using `tokio-rustls`. Both the
control connection and each data connection complete TLS before sending the
existing NES identification and framed CBOR messages.

For administrator requirements, an N-worker Kubernetes example, existing CA
integration, and certificate rotation, see the
[worker mTLS deployment guide](../docs/guide/worker-network-tls.md).

## Configuration

Example worker YAML:

```yaml
data_address: worker-a:9090
worker:
  network:
    tls:
      enabled: true
      certificate_file: /run/nes/tls/worker-a.pem
      private_key_file: /run/nes/tls/worker-a.key
      ca_file: /run/nes/tls/ca.pem
      handshake_timeout_ms: 10000
```

These options are also available as `--worker.network.tls.enabled=true`, etc.
All communicating workers must use the same TLS mode.

- `certificate_file`: PEM leaf certificate followed by any intermediate certificates,
  used for both server and client authentication.
- `private_key_file`: matching unencrypted PEM private key (PKCS#8, PKCS#1, or SEC1).
- `ca_file`: PEM bundle of trusted CA certificates for both receiving and connecting workers.
- `handshake_timeout_ms`: positive timeout for incoming TLS handshakes. For outgoing
  connections it covers DNS resolution, TCP connection, and the TLS handshake together.

The receiving worker's certificate must have a DNS or IP subject alternative name
matching its advertised `data_address`. DNS names are checked before resolution;
connecting by IP requires an IP SAN. Publicly issued certificates are not required:
a deployment-local CA and a leaf certificate for each worker are sufficient.
Distribute only the public CA certificate to workers, alongside their own leaf
certificate and private key. The CA signing key stays with the certificate issuer.

Every worker presents its certificate when accepting and initiating connections.
Issue worker certificates with both `serverAuth` and `clientAuth` extended key
usages. Senders verify the receiving worker's certificate chain, hostname, validity,
and server-authentication usage. Receivers require a client certificate and verify
its chain, validity, and client-authentication usage against the same CA bundle.
Clients with no certificate or an untrusted certificate are rejected.

Any valid client certificate trusted by the configured CA bundle is accepted;
this does not bind its identity to an NES worker ID or apply per-worker authorization.

TLS defaults to disabled for compatibility. Specifying certificate paths with TLS
disabled is rejected, as are missing/invalid credentials, mismatched keys, and a zero
timeout when TLS is enabled. TLS never falls back to plaintext. Enabling TLS while
using the in-process MemCom transport is rejected rather than bypassing TLS in tests.

## Implementation

`NetworkTlsConfiguration` is nested under `WorkerNetworkConfiguration`. Worker
startup converts its settings to `NetworkTlsOptions`, a `std::variant<NoTLS, TLS>`.
`NoTLS` is empty; `TLS` holds three `std::filesystem::path` values and a
`std::chrono::milliseconds` handshake timeout. Rust uses an opaque
`NetworkTlsOptions` enum with `NoTls` and `Tls` variants; `Tls` holds three
`PathBuf` values and a `Duration`. CXX factory functions construct this enum from
the C++ variant, and service startup borrows it separately from channel options.
`TlsCommunication::from_pem_files` receives paths and a duration directly.
Each service loads its credentials at startup and shares its rustls configuration
across that service's connections.

`CommunicationListener::listen` now returns an `IncomingConnection`. TCP and
MemCom return an already usable `Channel`; TLS returns an accepted socket whose
handshake runs in the receiver's connection task. A slow or malformed handshake
therefore cannot block the accept loop or terminate the listener. TLS limits pending
handshakes to 128 per listener; excess connections are dropped. Completed connection
task handles are removed as new connections arrive.

Two pre-existing sender event loops were awaited inside logging macros. They now
run independently of whether DEBUG/INFO logging is enabled; the service tests use
WARN logging to cover this behavior.

The existing framing, application acknowledgments, backpressure, and application
close messages operate on the TLS reader/writer halves. Both control and data
connections select the same transport at worker startup.

## Validation and remaining scope

The Rust tests generate their own CA and worker certificates in temporary directories.
They cover framed buffers larger than a TLS record, acknowledgments, DNS/IP SANs,
unknown CAs, name mismatches, missing/untrusted client certificates, client certificate
usage, invalid credentials, incoming/outgoing timeouts,
listener progress after bad clients, actual sender/receiver services with small
queues, and the existing TCP/MemCom transports.

```sh
cargo test --manifest-path nes-network/Cargo.toml -p nes_network -p network_bindings --lib
```

The `worker-mtls-e2e-test` Bats suite starts two worker containers with separate
certificates generated for the test. A generator source on one worker sends 10,000
rows to a file sink on the other; the test waits for both query fragments to stop
and compares every output value. A second case verifies that a worker certificate
issued by an untrusted CA is rejected and no query rows reach the sink.
Two further cases kill the sender or receiver with `SIGKILL` during an active
transfer. They check that the surviving worker continues answering health RPCs
without restarting, then restart the killed peer and verify all 10,000 rows of a
new distributed query. Recovery of the interrupted query is not asserted.
The suite requires Docker Compose, the existing Bats utilities, and the `openssl`
command on the test host. Build `nes-single-node-worker` and `nes-cli`, then run:

```sh
ctest --test-dir <build-directory> -R '^worker-mtls-e2e-test$' --output-on-failure
```

This is an implementation sketch with a few deployment concerns still open:

- Certificates are supplied externally and loaded at startup; rotation requires
  restarting the services. There is no automatic issuance or reload.
- The existing service retry policy also retries certificate failures; classifying
  permanent TLS errors and reporting query startup failures needs a follow-up.
- Application shutdown retains the existing NES Close-message behavior. A complete
  TLS `close_notify` exchange is not yet explicitly driven by the service handlers.
- The pending-handshake limit does not limit established connections or peers that
  complete TLS and then stall during NES identification.
- This covers the worker network data port, including its internal control channel.
  The separate gRPC API and external TCP source plugin are outside this change.
- Broader distributed scenarios, load testing, and throughput measurements remain to be done.
