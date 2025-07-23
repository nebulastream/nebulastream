use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, lookup_host};
use tokio_serde::Framed;
use tokio_serde::formats::Json;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};

pub type ChannelIdentifier = String;

pub type Result<T> = std::result::Result<T, crate::sender::Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionIdentifier(String);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ThisConnectionIdentifier(String);
impl Display for ConnectionIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl Display for ThisConnectionIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl ConnectionIdentifier {
    pub async fn to_socket_address(&self) -> Result<SocketAddr> {
        lookup_host(&self.0)
            .await
            .map_err(|e| format!("Could not resolve host. DNS Lookup failed: {e:?}"))?
            .filter(|addr| addr.is_ipv4())
            .next()
            .ok_or(format!("Could not resolve host: {:?}", self.0).into())
    }
}
impl Into<ConnectionIdentifier> for ThisConnectionIdentifier {
    fn into(self) -> ConnectionIdentifier {
        ConnectionIdentifier(self.0)
    }
}

impl From<String> for ThisConnectionIdentifier {
    fn from(value: String) -> Self {
        ThisConnectionIdentifier(value)
    }
}

impl From<String> for ConnectionIdentifier {
    fn from(value: String) -> Self {
        ConnectionIdentifier(value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ControlChannelRequest {
    ChannelRequest(ChannelIdentifier),
}
#[derive(Debug, Serialize, Deserialize)]
pub enum ControlChannelResponse {
    OkChannelResponse(u16),
    DenyChannelResponse,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum DataChannelRequest {
    Data(TupleBuffer),
    Close,
}
pub type Sequence = (u64, u64);

#[derive(Debug, Serialize, Deserialize)]
pub enum DataChannelResponse {
    AckData(Sequence),
    NAckData(Sequence),
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
    pub data: Vec<u8>,
    pub child_buffers: Vec<Vec<u8>>,
}

impl TupleBuffer {
    pub fn sequence(&self) -> Sequence {
        (self.sequence_number, self.chunk_number)
    }
}

impl Debug for TupleBuffer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("TupleBuffer{{ sequence_number: {}, origin_id: {}, chunk_number: {}, watermark: {}, number_of_tuples: {}, bufferSize: {}, children: {:?}}}", self.sequence_number, self.origin_id, self.chunk_number, self.watermark, self.number_of_tuples, self.data.len(), self.child_buffers.iter().map(|buffer| buffer.len()).collect::<Vec<_>>()))
    }
}

pub type DataChannelSenderReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    DataChannelResponse,
    DataChannelResponse,
    Json<DataChannelResponse, DataChannelResponse>,
>;
pub type DataChannelSenderWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    DataChannelRequest,
    DataChannelRequest,
    Json<DataChannelRequest, DataChannelRequest>,
>;
pub type DataChannelReceiverReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    DataChannelRequest,
    DataChannelRequest,
    Json<DataChannelRequest, DataChannelRequest>,
>;
pub type DataChannelReceiverWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    DataChannelResponse,
    DataChannelResponse,
    Json<DataChannelResponse, DataChannelResponse>,
>;

pub type ControlChannelSenderReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ControlChannelResponse,
    ControlChannelResponse,
    Json<ControlChannelResponse, ControlChannelResponse>,
>;
pub type ControlChannelSenderWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    ControlChannelRequest,
    ControlChannelRequest,
    Json<ControlChannelRequest, ControlChannelRequest>,
>;
pub type ControlChannelReceiverReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ControlChannelRequest,
    ControlChannelRequest,
    Json<ControlChannelRequest, ControlChannelRequest>,
>;
pub type ControlChannelReceiverWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    ControlChannelResponse,
    ControlChannelResponse,
    Json<ControlChannelResponse, ControlChannelResponse>,
>;

#[derive(Debug, Serialize, Deserialize)]
pub enum IdentificationResponse {
    Ok,
    NotOk,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum IdentificationRequest {
    IAmConnection(ThisConnectionIdentifier),
    IAmChannel(ThisConnectionIdentifier, ChannelIdentifier),
}
pub type IdentificationSenderReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    IdentificationResponse,
    IdentificationResponse,
    Json<IdentificationResponse, IdentificationResponse>,
>;
pub type IdentificationSenderWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    IdentificationRequest,
    IdentificationRequest,
    Json<IdentificationRequest, IdentificationRequest>,
>;
pub type IdentificationReceiverReader = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    IdentificationRequest,
    IdentificationRequest,
    Json<IdentificationRequest, IdentificationRequest>,
>;
pub type IdentificationReceiverWriter = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    IdentificationResponse,
    IdentificationResponse,
    Json<IdentificationResponse, IdentificationResponse>,
>;

pub fn data_channel_sender(
    stream: TcpStream,
) -> (DataChannelSenderReader, DataChannelSenderWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Json::<DataChannelResponse, DataChannelResponse>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Json::<DataChannelRequest, DataChannelRequest>::default(),
    );

    (read, write)
}

pub fn data_channel_receiver(
    stream: TcpStream,
) -> (DataChannelReceiverReader, DataChannelReceiverWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Json::<DataChannelRequest, DataChannelRequest>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Json::<DataChannelResponse, DataChannelResponse>::default(),
    );

    (read, write)
}

pub fn control_channel_sender(
    stream: TcpStream,
) -> (ControlChannelSenderReader, ControlChannelSenderWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Json::<ControlChannelResponse, ControlChannelResponse>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Json::<ControlChannelRequest, ControlChannelRequest>::default(),
    );

    (read, write)
}

pub fn control_channel_receiver(
    stream: TcpStream,
) -> (ControlChannelReceiverReader, ControlChannelReceiverWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Json::<ControlChannelRequest, ControlChannelRequest>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Json::<ControlChannelResponse, ControlChannelResponse>::default(),
    );

    (read, write)
}

pub fn identification_sender(
    stream: TcpStream,
) -> (IdentificationSenderReader, IdentificationSenderWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Json::<IdentificationResponse, IdentificationResponse>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Json::<IdentificationRequest, IdentificationRequest>::default(),
    );

    (read, write)
}

pub fn identification_receiver(
    stream: TcpStream,
) -> (IdentificationReceiverReader, IdentificationReceiverWriter) {
    let (read, write) = stream.into_split();
    let read = FramedRead::new(read, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(
        read,
        Json::<IdentificationRequest, IdentificationRequest>::default(),
    );

    let write = FramedWrite::new(write, LengthDelimitedCodec::new());
    let write = tokio_serde::Framed::new(
        write,
        Json::<IdentificationResponse, IdentificationResponse>::default(),
    );

    (read, write)
}
