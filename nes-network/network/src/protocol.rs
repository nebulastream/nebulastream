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

use crate::channel::Channel;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::str::FromStr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::lookup_host;
use tokio_serde::Framed;
use tokio_serde::formats::Cbor;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};
use url::{Host, Url};

pub type ChannelIdentifier = String;

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Identifies a remote connection endpoint (the target we want to connect to).
/// Used when establishing outgoing connections to other workers.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionIdentifier(Url);
/// Identifies this local connection endpoint (our own address).
/// Used when setting up local services that accept incoming connections.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ThisConnectionIdentifier(Url);
impl Display for ConnectionIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}
impl Display for ThisConnectionIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

impl ConnectionIdentifier {
    pub async fn to_socket_address(&self) -> Result<SocketAddr> {
        let port = self.0.port().expect("Checked");
        match self.0.host().expect("Checked") {
            Host::Domain(s) => lookup_host(format!("{s}:{port}"))
                .await
                .map_err(|e| format!("Could not resolve host. DNS Lookup failed: {e:?}"))?
                .find(|addr| addr.is_ipv4())
                .ok_or(format!("Could not resolve host: {:?}", self.0).into()),
            Host::Ipv4(ip) => Ok(SocketAddr::new(ip.into(), port)),
            Host::Ipv6(ip) => Ok(SocketAddr::new(ip.into(), port)),
        }
    }
}
impl Into<ConnectionIdentifier> for ThisConnectionIdentifier {
    fn into(self) -> ConnectionIdentifier {
        ConnectionIdentifier(self.0)
    }
}

impl FromStr for ThisConnectionIdentifier {
    type Err = Box<dyn std::error::Error + Send + Sync>;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let connection = ConnectionIdentifier::from_str(s)?;
        Ok(ThisConnectionIdentifier(connection.0))
    }
}

impl FromStr for ConnectionIdentifier {
    type Err = Box<dyn std::error::Error + Send + Sync>;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let url = Url::parse(&format!("nes://{s}"))
            .map_err(|e| format!("Invalid ConnectionIdentifier: Invalid Url: {e}"))?;
        url.host().ok_or("Invalid ConnectionIdentifier: No host")?;
        url.port().ok_or("Invalid ConnectionIdentifier: No port")?;
        if url.scheme() != "nes" {
            return Err("Invalid ConnectionIdentifier: Invalid scheme".into());
        }

        Ok(ConnectionIdentifier(url))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlChannelRequest {
    ChannelRequest(ChannelIdentifier),
}
#[derive(Debug, Serialize, Deserialize)]
pub enum ControlChannelResponse {
    OkChannelResponse(ConnectionIdentifier),
    DenyChannelResponse,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum DataChannelRequest {
    Data(TupleBuffer),
    Close,
}

/// This represents the per Origin SequenceNumber
/// It's a triplet of OriginId, SequenceNumber, ChunkNumber
/// This triplet is assumed to be unique within the context of a single data channel
pub type OriginSequenceNumber = (u64, u64, u64);

#[derive(Debug, Serialize, Deserialize)]
pub enum DataChannelResponse {
    AckData(OriginSequenceNumber),
    NAckData(OriginSequenceNumber),
    Close,
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct TupleBuffer {
    pub sequence_number: u64,
    pub origin_id: u64,
    pub watermark: u64,
    pub chunk_number: u64,
    pub number_of_tuples: u64,
    pub last_chunk: bool,
    pub encoded_data: bool,
    pub encoded_children: Vec<bool>,
    pub data: Vec<u8>,
    pub child_buffers: Vec<Vec<u8>>,
}

impl TupleBuffer {
    pub fn sequence(&self) -> OriginSequenceNumber {
        (self.origin_id, self.sequence_number, self.chunk_number)
    }
}

impl Debug for TupleBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("TupleBuffer{{ sequence_number: {}, origin_id: {}, chunk_number: {}, watermark: {}, number_of_tuples: {}, encoded_data: {}, bufferSize: {}, encoded_children: {:?}, children: {:?}}}", self.sequence_number, self.origin_id, self.chunk_number, self.watermark, self.number_of_tuples, self.encoded_data, self.data.len(), self.encoded_children, self.child_buffers.iter().map(|buffer| buffer.len()).collect::<Vec<_>>()))
    }
}

pub type DataChannelSenderReader<R> = Framed<
    FramedRead<R, LengthDelimitedCodec>,
    DataChannelResponse,
    DataChannelResponse,
    Cbor<DataChannelResponse, DataChannelResponse>,
>;
pub type DataChannelSenderWriter<W> = Framed<
    FramedWrite<W, LengthDelimitedCodec>,
    DataChannelRequest,
    DataChannelRequest,
    Cbor<DataChannelRequest, DataChannelRequest>,
>;
pub type DataChannelReceiverReader<R> = Framed<
    FramedRead<R, LengthDelimitedCodec>,
    DataChannelRequest,
    DataChannelRequest,
    Cbor<DataChannelRequest, DataChannelRequest>,
>;
pub type DataChannelReceiverWriter<W> = Framed<
    FramedWrite<W, LengthDelimitedCodec>,
    DataChannelResponse,
    DataChannelResponse,
    Cbor<DataChannelResponse, DataChannelResponse>,
>;

pub type ControlChannelSenderReader<R> = Framed<
    FramedRead<R, LengthDelimitedCodec>,
    ControlChannelResponse,
    ControlChannelResponse,
    Cbor<ControlChannelResponse, ControlChannelResponse>,
>;
pub type ControlChannelSenderWriter<W> = Framed<
    FramedWrite<W, LengthDelimitedCodec>,
    ControlChannelRequest,
    ControlChannelRequest,
    Cbor<ControlChannelRequest, ControlChannelRequest>,
>;
pub type ControlChannelReceiverReader<R> = Framed<
    FramedRead<R, LengthDelimitedCodec>,
    ControlChannelRequest,
    ControlChannelRequest,
    Cbor<ControlChannelRequest, ControlChannelRequest>,
>;
pub type ControlChannelReceiverWriter<W> = Framed<
    FramedWrite<W, LengthDelimitedCodec>,
    ControlChannelResponse,
    ControlChannelResponse,
    Cbor<ControlChannelResponse, ControlChannelResponse>,
>;

#[derive(Debug, Serialize, Deserialize)]
pub enum IdentificationResponse {
    Ok,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum IdentificationRequest {
    IAmConnection(ThisConnectionIdentifier),
    IAmChannel(ThisConnectionIdentifier, ChannelIdentifier),
}
pub type IdentificationSenderReader<R> = Framed<
    FramedRead<R, LengthDelimitedCodec>,
    IdentificationResponse,
    IdentificationResponse,
    Cbor<IdentificationResponse, IdentificationResponse>,
>;
pub type IdentificationSenderWriter<W> = Framed<
    FramedWrite<W, LengthDelimitedCodec>,
    IdentificationRequest,
    IdentificationRequest,
    Cbor<IdentificationRequest, IdentificationRequest>,
>;
pub type IdentificationReceiverReader<R> = Framed<
    FramedRead<R, LengthDelimitedCodec>,
    IdentificationRequest,
    IdentificationRequest,
    Cbor<IdentificationRequest, IdentificationRequest>,
>;
pub type IdentificationReceiverWriter<W> = Framed<
    FramedWrite<W, LengthDelimitedCodec>,
    IdentificationResponse,
    IdentificationResponse,
    Cbor<IdentificationResponse, IdentificationResponse>,
>;

pub fn data_channel_sender<R: AsyncRead + Send + Unpin, W: AsyncWrite + Send + Unpin>(
    stream: Channel<R, W>,
) -> (DataChannelSenderReader<R>, DataChannelSenderWriter<W>) {
    let read = FramedRead::new(stream.reader, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<DataChannelResponse, DataChannelResponse>::default(),
    );

    let write = FramedWrite::new(stream.writer, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<DataChannelRequest, DataChannelRequest>::default(),
    );

    (read, write)
}

pub fn data_channel_receiver<R: AsyncRead + Send + Unpin, W: AsyncWrite + Send + Unpin>(
    stream: Channel<R, W>,
) -> (DataChannelReceiverReader<R>, DataChannelReceiverWriter<W>) {
    let read = FramedRead::new(stream.reader, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<DataChannelRequest, DataChannelRequest>::default(),
    );

    let write = FramedWrite::new(stream.writer, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<DataChannelResponse, DataChannelResponse>::default(),
    );

    (read, write)
}

pub fn control_channel_sender<R: AsyncRead + Send + Unpin, W: AsyncWrite + Send + Unpin>(
    stream: Channel<R, W>,
) -> (ControlChannelSenderReader<R>, ControlChannelSenderWriter<W>) {
    let read = FramedRead::new(stream.reader, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<ControlChannelResponse, ControlChannelResponse>::default(),
    );

    let write = FramedWrite::new(stream.writer, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<ControlChannelRequest, ControlChannelRequest>::default(),
    );

    (read, write)
}

pub fn control_channel_receiver<R: AsyncRead + Send + Unpin, W: AsyncWrite + Send + Unpin>(
    stream: Channel<R, W>,
) -> (
    ControlChannelReceiverReader<R>,
    ControlChannelReceiverWriter<W>,
) {
    let read = FramedRead::new(stream.reader, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<ControlChannelRequest, ControlChannelRequest>::default(),
    );

    let write = FramedWrite::new(stream.writer, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<ControlChannelResponse, ControlChannelResponse>::default(),
    );

    (read, write)
}

pub fn identification_sender<R: AsyncRead + Send + Unpin, W: AsyncWrite + Send + Unpin>(
    stream: Channel<R, W>,
) -> (IdentificationSenderReader<R>, IdentificationSenderWriter<W>) {
    let read = FramedRead::new(stream.reader, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<IdentificationResponse, IdentificationResponse>::default(),
    );

    let write = FramedWrite::new(stream.writer, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<IdentificationRequest, IdentificationRequest>::default(),
    );

    (read, write)
}

pub fn identification_receiver<R: AsyncRead + Send + Unpin, W: AsyncWrite + Send + Unpin>(
    stream: Channel<R, W>,
) -> (
    IdentificationReceiverReader<R>,
    IdentificationReceiverWriter<W>,
) {
    let read = FramedRead::new(stream.reader, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Cbor::<IdentificationRequest, IdentificationRequest>::default(),
    );

    let write = FramedWrite::new(stream.writer, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Cbor::<IdentificationResponse, IdentificationResponse>::default(),
    );

    (read, write)
}

#[test]
fn test() {
    assert!(ConnectionIdentifier::from_str("tcp://localhost:8080").is_err());
    assert!(ConnectionIdentifier::from_str("localhost").is_err());
    assert!(ConnectionIdentifier::from_str("localhost:ABBB").is_err());
    assert!(ConnectionIdentifier::from_str("yoo:localhost:ABBB").is_err());
    assert!(ConnectionIdentifier::from_str("localhost:8080").is_ok());
    assert!(ConnectionIdentifier::from_str("127.0.0.1:8080").is_ok());
    assert!(ConnectionIdentifier::from_str("google.dot.com:8080").is_ok());
}
