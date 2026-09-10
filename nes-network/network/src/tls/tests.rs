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

use super::*;
use crate::protocol::{
    DataChannelRequest, DataChannelResponse, TupleBuffer, data_channel_receiver,
    data_channel_sender,
};
use futures::{SinkExt, StreamExt};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Authority {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

impl Authority {
    fn new() -> Result<Self> {
        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate()?;
        let ca_cert = ca_params.self_signed(&ca_key)?;
        Ok(Self {
            issuer: Issuer::new(ca_params, ca_key),
            pem: ca_cert.pem(),
        })
    }

    fn identity(&self, names: &[&str], usages: Vec<ExtendedKeyUsagePurpose>) -> Result<Identity> {
        let directory = tempfile::tempdir()?;
        let mut params =
            CertificateParams::new(names.iter().map(|s| s.to_string()).collect::<Vec<_>>())?;
        params.extended_key_usages = usages;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let key = KeyPair::generate()?;
        let cert = params.signed_by(&key, &self.issuer)?;
        std::fs::write(directory.path().join("ca.pem"), &self.pem)?;
        std::fs::write(directory.path().join("worker.pem"), cert.pem())?;
        std::fs::write(directory.path().join("worker.key"), key.serialize_pem())?;
        Ok(Identity { directory })
    }
}

struct Identity {
    directory: TempDir,
}

impl Identity {
    fn new(names: &[&str]) -> Result<Self> {
        Authority::new()?.identity(
            names,
            vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ],
        )
    }

    fn transport(&self, limit: Duration) -> Result<TlsCommunication> {
        TlsCommunication::from_pem_files(
            &self.directory.path().join("worker.pem"),
            &self.directory.path().join("worker.key"),
            &self.directory.path().join("ca.pem"),
            limit,
        )
    }
}

async fn listener(transport: &mut TlsCommunication) -> Result<(TlsCommunicationListener, u16)> {
    let listener = transport.bind("127.0.0.1:0".parse()?).await?;
    let port = listener.listener.local_addr()?.port();
    Ok((listener, port))
}

#[tokio::test]
async fn transfers_framed_buffers_and_acknowledgments_over_tls() -> Result<()> {
    // DNS-only SAN: verification must use localhost, not its resolved IP.
    let authority = Authority::new()?;
    let usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let identity = authority.identity(&["localhost"], usages.clone())?;
    let sending_identity = authority.identity(&["sending-worker"], usages)?;
    let mut transport = identity.transport(Duration::from_secs(2))?;
    let sending_transport = sending_identity.transport(Duration::from_secs(2))?;
    let (mut listener, port) = listener(&mut transport).await?;
    let target = format!("localhost:{port}").parse()?;
    let (client, server) = tokio::try_join!(sending_transport.connect(&target), async {
        listener.listen().await?.establish().await
    },)?;
    let (mut acks, mut send) = data_channel_sender(client);
    let (mut receive, mut ack) = data_channel_receiver(server);
    let buffer = TupleBuffer {
        sequence_number: 42,
        origin_id: 1,
        watermark: 7,
        chunk_number: 0,
        number_of_tuples: 1,
        last_chunk: true,
        data: vec![0xab; 256 * 1024],
        child_buffers: vec![vec![0xcd; 32 * 1024]],
    };
    timeout(Duration::from_secs(3), async {
        tokio::try_join!(
            async {
                send.send(DataChannelRequest::Data(buffer.clone())).await?;
                match acks.next().await.ok_or("missing ack")?? {
                    DataChannelResponse::AckData(sequence) => {
                        assert_eq!(sequence, buffer.sequence())
                    }
                    other => panic!("Unexpected response: {other:?}"),
                }
                send.send(DataChannelRequest::Close).await?;
                Result::Ok(())
            },
            async {
                match receive.next().await.ok_or("missing buffer")?? {
                    DataChannelRequest::Data(received) => assert_eq!(received, buffer),
                    other => panic!("Unexpected request: {other:?}"),
                }
                ack.send(DataChannelResponse::AckData(buffer.sequence()))
                    .await?;
                assert!(matches!(
                    receive.next().await.ok_or("missing close")??,
                    DataChannelRequest::Close
                ));
                Result::Ok(())
            },
        )?;
        Result::Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn verifies_ip_subject_alternative_names_and_clean_shutdown() -> Result<()> {
    let identity = Identity::new(&["127.0.0.1"])?;
    let mut transport = identity.transport(Duration::from_secs(2))?;
    let (mut listener, port) = listener(&mut transport).await?;
    let target = format!("127.0.0.1:{port}").parse()?;
    let (mut client, mut server) = tokio::try_join!(transport.connect(&target), async {
        listener.listen().await?.establish().await
    },)?;
    client.writer.write_all(b"hello").await?;
    client.writer.shutdown().await?;
    let mut data = Vec::new();
    timeout(Duration::from_secs(2), server.reader.read_to_end(&mut data)).await??;
    assert_eq!(data, b"hello");
    Ok(())
}

#[tokio::test]
async fn rejects_wrong_hostname_and_untrusted_ca() -> Result<()> {
    for wrong_name in [true, false] {
        let identity = Identity::new(&["localhost"])?;
        let other_identity = Identity::new(&["localhost"])?;
        let mut server_transport = identity.transport(Duration::from_secs(2))?;
        let client_transport = if wrong_name {
            identity.transport(Duration::from_secs(2))?
        } else {
            other_identity.transport(Duration::from_secs(2))?
        };
        let (mut listener, port) = listener(&mut server_transport).await?;
        let host = if wrong_name { "127.0.0.1" } else { "localhost" };
        let target = format!("{host}:{port}").parse()?;
        let (client, _) = tokio::join!(client_transport.connect(&target), async {
            listener.listen().await?.establish().await
        },);
        let error = client
            .err()
            .expect("certificate must be rejected")
            .to_string();
        assert!(error.contains("certificate"), "Unexpected error: {error}");
    }
    Ok(())
}

#[tokio::test]
async fn rejects_missing_untrusted_and_wrong_usage_client_certificates() -> Result<()> {
    let authority = Authority::new()?;
    let identity = authority.identity(
        &["localhost"],
        vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ],
    )?;
    let untrusted = Identity::new(&["sending-worker"])?;
    let server_only = authority.identity(
        &["sending-worker"],
        vec![ExtendedKeyUsagePurpose::ServerAuth],
    )?;
    for (case, client_identity) in [
        ("missing", None),
        ("untrusted", Some(&untrusted)),
        ("wrong usage", Some(&server_only)),
    ] {
        let mut transport = identity.transport(Duration::from_secs(2))?;
        let (mut listener, port) = listener(&mut transport).await?;
        // Every client trusts the server; only its own certificate varies.
        let mut roots = RootCertStore::empty();
        for cert in certificates(&identity.directory.path().join("ca.pem"))? {
            roots.add(cert)?;
        }
        let builder =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])?
                .with_root_certificates(roots);
        let config = if let Some(client_identity) = client_identity {
            let chain = certificates(&client_identity.directory.path().join("worker.pem"))?;
            let key_file = File::open(client_identity.directory.path().join("worker.key"))?;
            let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
                .ok_or("missing test private key")?;
            builder.with_client_auth_cert(chain, key)?
        } else {
            builder.with_no_client_auth()
        };
        let connector = TlsConnector::from(Arc::new(config));
        let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
        let name = ServerName::try_from("localhost")?;
        let (_client, server) = timeout(Duration::from_secs(3), async {
            tokio::join!(connector.connect(name, tcp), async {
                listener.listen().await?.establish().await
            })
        })
        .await?;
        // TLS 1.3 clients may finish before seeing the server's rejection alert.
        let error = server
            .err()
            .expect("server must reject the client certificate");
        let tls_error = error
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .and_then(|error| error.downcast_ref::<rustls::Error>())
            .expect("TLS rejection error");
        match case {
            "missing" => assert!(matches!(tls_error, rustls::Error::NoCertificatesPresented)),
            "untrusted" => assert!(matches!(
                tls_error,
                rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer)
            )),
            "wrong usage" => assert!(matches!(
                tls_error,
                rustls::Error::InvalidCertificate(
                    rustls::CertificateError::InvalidPurpose
                        | rustls::CertificateError::InvalidPurposeContext { .. }
                )
            )),
            _ => unreachable!(),
        }
        assert_eq!(
            listener.pending_handshakes.available_permits(),
            MAX_PENDING_HANDSHAKES
        );
    }
    Ok(())
}

#[tokio::test]
async fn slow_or_invalid_handshakes_do_not_block_new_connections() -> Result<()> {
    let identity = Identity::new(&["localhost"])?;
    let mut transport = identity.transport(Duration::from_secs(2))?;
    let (mut listener, port) = listener(&mut transport).await?;
    let raw = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
    let pending = listener.listen().await?;
    let slow = tokio::spawn(pending.establish());
    let mut invalid = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
    invalid.write_all(b"this is not a TLS handshake").await?;
    let invalid_handshake = tokio::spawn(listener.listen().await?.establish());
    let target = format!("localhost:{port}").parse()?;
    timeout(Duration::from_secs(1), async {
        tokio::try_join!(transport.connect(&target), async {
            listener.listen().await?.establish().await
        },)
    })
    .await??;
    assert!(
        !slow.is_finished(),
        "healthy connection must finish before stalled handshake"
    );
    assert!(invalid_handshake.await?.is_err());
    drop(raw);
    assert!(slow.await?.is_err());
    assert_eq!(
        listener.pending_handshakes.available_permits(),
        MAX_PENDING_HANDSHAKES
    );
    Ok(())
}

#[tokio::test]
async fn times_out_incoming_and_outgoing_handshakes() -> Result<()> {
    let identity = Identity::new(&["localhost"])?;
    let mut transport = identity.transport(Duration::from_millis(100))?;
    let (mut listener, port) = listener(&mut transport).await?;
    let _raw = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
    let incoming = listener.listen().await?;
    let error = timeout(Duration::from_secs(2), incoming.establish())
        .await?
        .err()
        .expect("stalled incoming handshake must fail")
        .to_string();
    assert!(error.contains("timed out"));
    assert_eq!(
        listener.pending_handshakes.available_permits(),
        MAX_PENDING_HANDSHAKES
    );

    let raw_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let target = format!("localhost:{}", raw_listener.local_addr()?.port()).parse()?;
    let (client, accepted) = tokio::join!(transport.connect(&target), raw_listener.accept());
    let _accepted = accepted?;
    let error = client
        .err()
        .expect("stalled outgoing handshake must fail")
        .to_string();
    assert!(error.contains("timed out"));
    Ok(())
}

#[test]
fn rejects_missing_empty_and_mismatched_credentials() -> Result<()> {
    let identity = Identity::new(&["localhost"])?;
    assert!(identity.transport(Duration::ZERO).is_err());
    assert!(
        TlsCommunication::from_pem_files(
            Path::new(""),
            Path::new(""),
            Path::new(""),
            Duration::from_secs(1)
        )
        .is_err()
    );
    std::fs::write(
        identity.directory.path().join("worker.key"),
        KeyPair::generate()?.serialize_pem(),
    )?;
    assert!(identity.transport(Duration::from_secs(1)).is_err());
    std::fs::write(identity.directory.path().join("ca.pem"), "")?;
    assert!(identity.transport(Duration::from_secs(1)).is_err());
    std::fs::remove_file(identity.directory.path().join("worker.pem"))?;
    assert!(identity.transport(Duration::from_secs(1)).is_err());
    Ok(())
}

fn service_roundtrip<C: Communication + 'static>(
    transport: C,
    inject_bad_clients: bool,
) -> Result<()> {
    use crate::receiver::{NetworkService as ReceiverService, ReceiverChannelResult};
    use crate::sender::{NetworkService as SenderService, SenderConfig, TrySendDataResult};
    use std::io::Write;
    use std::thread;

    // The services must run even when their DEBUG/INFO log statements are disabled.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    // The public service API does not expose a bound port, so reserve an ephemeral port first.
    let reservation = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
    let port = reservation.local_addr()?.port();
    let endpoint = format!("localhost:{port}");
    let runtime = || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
    };
    drop(reservation);
    let receiver = ReceiverService::start(runtime()?, endpoint.parse()?, transport.clone());
    let input = receiver.register_channel("tls-test".to_owned(), 2)?;
    let mut bad_clients = Vec::new();
    if inject_bad_clients {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let stalled = loop {
            match std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port)) {
                Ok(stream) => break stream,
                Err(_) if std::time::Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5))
                }
                Err(e) => return Err(e.into()),
            }
        };
        bad_clients.push(stalled);
        let mut malformed = std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))?;
        malformed.write_all(b"not a TLS handshake")?;
        bad_clients.push(malformed);
    }
    let sender = SenderService::start(runtime()?, "localhost:0".parse()?, transport);
    let output = sender.register_channel(
        endpoint.parse()?,
        "tls-test".to_owned(),
        SenderConfig {
            sender_queue_size: 2,
            max_pending_acks: 2,
        },
    )?;
    let consume = thread::spawn(move || -> Result<()> {
        for sequence in 0..32 {
            match input.receive() {
                ReceiverChannelResult::Ok(buffer) => {
                    assert_eq!(buffer.sequence_number, sequence);
                    assert_eq!(buffer.data, vec![sequence as u8; 64 * 1024]);
                }
                _ => panic!("Channel closed before all buffers arrived"),
            }
        }
        assert!(matches!(input.receive(), ReceiverChannelResult::Closed));
        Ok(())
    });
    for sequence in 0..32 {
        let mut buffer = TupleBuffer {
            sequence_number: sequence,
            origin_id: 1,
            watermark: 0,
            chunk_number: 0,
            number_of_tuples: 1,
            last_chunk: true,
            data: vec![sequence as u8; 64 * 1024],
            child_buffers: vec![],
        };
        loop {
            match output.try_send_data(buffer) {
                TrySendDataResult::Ok => break,
                TrySendDataResult::Full(returned) => {
                    buffer = returned;
                    thread::yield_now();
                }
                TrySendDataResult::Closed(_) => return Err("Sender closed unexpectedly".into()),
            }
        }
    }
    while !output.flush()? {
        thread::yield_now();
    }
    output.close();
    consume.join().expect("receiver thread panicked")?;
    drop(bad_clients);
    sender.shutdown()?;
    receiver.shutdown()?;
    Ok(())
}

#[test]
fn network_services_transfer_with_backpressure_after_bad_tls_connections() -> Result<()> {
    let identity = Identity::new(&["localhost"])?;
    let transport = identity.transport(Duration::from_secs(2))?;
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        let _ = send.send(service_roundtrip(transport, true));
    });
    receive.recv_timeout(Duration::from_secs(10))??;
    task.join().expect("service thread panicked");
    Ok(())
}

#[test]
fn existing_tcp_and_memcom_services_still_transfer() -> Result<()> {
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let task = std::thread::spawn(move || {
        let result = service_roundtrip(crate::channel::TcpCommunication::new(), false)
            .and_then(|_| service_roundtrip(crate::channel::MemCom::new(), false));
        let _ = send.send(result);
    });
    receive.recv_timeout(Duration::from_secs(10))??;
    task.join().expect("service thread panicked");
    Ok(())
}
