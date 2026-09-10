/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

//! Mutual TLS 1.3 over the worker TCP transport. Certificates are loaded once at service startup.

use crate::channel::{Channel, Communication, CommunicationListener, IncomingConnection, Result};
use crate::protocol::{ConnectionIdentifier, ThisConnectionIdentifier};
use std::fs::File;
use std::io::BufReader;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{ReadHalf, WriteHalf, split};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use tracing::warn;

pub type TlsReader = ReadHalf<TlsStream<TcpStream>>;
pub type TlsWriter = WriteHalf<TlsStream<TcpStream>>;

// Bound the number of unfinished handshakes, including sockets accepted but not yet polled.
const MAX_PENDING_HANDSHAKES: usize = 128;

#[derive(Clone)]
pub struct TlsCommunication {
    connector: TlsConnector,
    acceptor: TlsAcceptor,
    handshake_timeout: Duration,
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .map_err(|e| format!("Cannot open TLS certificate file {}: {e}", path.display()))?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| format!("Cannot read TLS certificate file {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!(
            "TLS certificate file {} contains no certificates",
            path.display()
        )
        .into());
    }
    Ok(certs)
}

impl TlsCommunication {
    /// Workers present their certificate in both roles and verify peers using the supplied CA bundle.
    /// Client certificates are required. A TLS failure never falls back to plaintext.
    pub fn from_pem_files(
        certificate_file: &Path,
        private_key_file: &Path,
        ca_file: &Path,
        handshake_timeout: Duration,
    ) -> Result<Self> {
        if certificate_file.as_os_str().is_empty()
            || private_key_file.as_os_str().is_empty()
            || ca_file.as_os_str().is_empty()
        {
            return Err("TLS requires certificate_file, private_key_file and ca_file".into());
        }
        if handshake_timeout.is_zero() {
            return Err("TLS handshake_timeout_ms must be greater than zero".into());
        }
        let chain = certificates(certificate_file)?;
        let file = File::open(private_key_file).map_err(|e| {
            format!(
                "Cannot open TLS private key file {}: {e}",
                private_key_file.display()
            )
        })?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(file))
            .map_err(|e| {
                format!(
                    "Cannot read TLS private key file {}: {e}",
                    private_key_file.display()
                )
            })?
            .ok_or_else(|| {
                format!(
                    "TLS private key file {} contains no private key",
                    private_key_file.display()
                )
            })?;
        let mut roots = RootCertStore::empty();
        for cert in certificates(ca_file)? {
            roots
                .add(cert)
                .map_err(|e| format!("Invalid TLS CA certificate in {}: {e}", ca_file.display()))?;
        }
        // Select the provider explicitly rather than modifying the process-wide rustls provider.
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let roots = Arc::new(roots);
        let client_verifier =
            WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
                .build()
                .map_err(|e| format!("Invalid TLS client CA bundle: {e}"))?;
        let server = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(|e| format!("Invalid TLS certificate/private key: {e}"))?;
        let client = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map_err(|e| format!("Invalid TLS client certificate/private key: {e}"))?;
        Ok(Self {
            connector: TlsConnector::from(Arc::new(client)),
            acceptor: TlsAcceptor::from(Arc::new(server)),
            handshake_timeout,
        })
    }
}

pub struct TlsCommunicationListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    handshake_timeout: Duration,
    pending_handshakes: Arc<Semaphore>,
}

pub struct IncomingTlsConnection {
    stream: TcpStream,
    acceptor: TlsAcceptor,
    handshake_timeout: Duration,
    permit: OwnedSemaphorePermit,
}

impl IncomingConnection for IncomingTlsConnection {
    type Reader = TlsReader;
    type Writer = TlsWriter;

    async fn establish(self) -> Result<Channel<Self::Reader, Self::Writer>> {
        let _permit = self.permit;
        let stream = timeout(self.handshake_timeout, self.acceptor.accept(self.stream))
            .await
            .map_err(|_| "Incoming TLS handshake timed out")??;
        let (reader, writer) = split(TlsStream::from(stream));
        Ok(Channel { reader, writer })
    }
}

impl CommunicationListener for TlsCommunicationListener {
    type Reader = TlsReader;
    type Writer = TlsWriter;
    type Incoming = IncomingTlsConnection;

    async fn listen(&mut self) -> Result<Self::Incoming> {
        loop {
            let (stream, peer) = self.listener.accept().await?;
            let Ok(permit) = self.pending_handshakes.clone().try_acquire_owned() else {
                warn!("Rejecting TLS connection from {peer}: too many pending handshakes");
                continue;
            };
            return Ok(IncomingTlsConnection {
                stream,
                acceptor: self.acceptor.clone(),
                handshake_timeout: self.handshake_timeout,
                permit,
            });
        }
    }
}

impl Communication for TlsCommunication {
    type Listener = TlsCommunicationListener;
    type Reader = TlsReader;
    type Writer = TlsWriter;

    async fn bind(&mut self, identifier: ThisConnectionIdentifier) -> Result<Self::Listener> {
        let identifier: ConnectionIdentifier = identifier.into();
        let address = identifier.to_socket_address().await?;
        // Match the existing TCP listener's IPv4 wildcard binding.
        let listener =
            TcpListener::bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, address.port()))).await?;
        Ok(TlsCommunicationListener {
            listener,
            acceptor: self.acceptor.clone(),
            handshake_timeout: self.handshake_timeout,
            pending_handshakes: Arc::new(Semaphore::new(MAX_PENDING_HANDSHAKES)),
        })
    }

    async fn connect(
        &self,
        identifier: &ConnectionIdentifier,
    ) -> Result<Channel<Self::Reader, Self::Writer>> {
        // Keep the original hostname for verification; resolving it first would verify the IP instead.
        let name = ServerName::try_from(identifier.host())?;
        timeout(self.handshake_timeout, async {
            let address = identifier.to_socket_address().await?;
            let tcp = TcpStream::connect(address).await?;
            let stream = self.connector.connect(name, tcp).await?;
            let (reader, writer) = split(TlsStream::from(stream));
            Ok(Channel { reader, writer })
        })
        .await
        .map_err(|_| format!("TLS connection to {identifier} timed out"))?
    }
}

#[cfg(test)]
mod tests;
