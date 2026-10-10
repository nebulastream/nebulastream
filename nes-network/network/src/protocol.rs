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
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::lookup_host;
use tokio_serde::formats::Cbor;
use tokio_serde::{Deserializer, Framed, Serializer};
use tokio_util::bytes::{Buf, BufMut, Bytes, BytesMut};
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
        f.write_fmt(format_args!("TupleBuffer{{ sequence_number: {}, origin_id: {}, chunk_number: {}, watermark: {}, number_of_tuples: {}, bufferSize: {}, children: {:?}}}", self.sequence_number, self.origin_id, self.chunk_number, self.watermark, self.number_of_tuples, self.data.len(), self.child_buffers.iter().map(|buffer| buffer.len()).collect::<Vec<_>>()))
    }
}

/// Binary codec for the data channel direction sender -> receiver.
///
/// Frame layout (the length prefix is added by the `LengthDelimitedCodec`):
///
/// ```text
/// Close: u8 tag = 0
/// Data:  u8 tag = 1, u64 sequence_number, u64 origin_id, u64 watermark, u64 chunk_number, u64 number_of_tuples,
///        u8 last_chunk, u32 data_len, u32 child_count, data bytes, child_count * (u32 child_len, child bytes)
/// ```
#[derive(Debug, Default)]
pub struct RawDataRequestCodec;

const TAG_CLOSE: u8 = 0;
const TAG_DATA: u8 = 1;
/// tag + 5 * u64 + last_chunk + data_len + child_count
const DATA_HEADER_LEN: usize = 1 + 5 * 8 + 1 + 4 + 4;

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

fn length_as_u32(len: usize) -> io::Result<u32> {
    u32::try_from(len).map_err(|_| invalid_data("buffer is larger than 4GiB"))
}

impl Serializer<DataChannelRequest> for RawDataRequestCodec {
    type Error = io::Error;

    fn serialize(
        self: Pin<&mut Self>,
        item: &DataChannelRequest,
    ) -> std::result::Result<Bytes, Self::Error> {
        match item {
            DataChannelRequest::Close => Ok(Bytes::from_static(&[TAG_CLOSE])),
            DataChannelRequest::Data(buffer) => {
                let payload_len = buffer.data.len()
                    + buffer
                        .child_buffers
                        .iter()
                        .map(|child| 4 + child.len())
                        .sum::<usize>();
                let mut out = BytesMut::with_capacity(DATA_HEADER_LEN + payload_len);
                out.put_u8(TAG_DATA);
                out.put_u64_le(buffer.sequence_number);
                out.put_u64_le(buffer.origin_id);
                out.put_u64_le(buffer.watermark);
                out.put_u64_le(buffer.chunk_number);
                out.put_u64_le(buffer.number_of_tuples);
                out.put_u8(buffer.last_chunk as u8);
                out.put_u32_le(length_as_u32(buffer.data.len())?);
                out.put_u32_le(length_as_u32(buffer.child_buffers.len())?);
                out.put_slice(&buffer.data);
                for child in &buffer.child_buffers {
                    out.put_u32_le(length_as_u32(child.len())?);
                    out.put_slice(child);
                }
                Ok(out.freeze())
            }
        }
    }
}

impl Deserializer<DataChannelRequest> for RawDataRequestCodec {
    type Error = io::Error;

    fn deserialize(
        self: Pin<&mut Self>,
        src: &BytesMut,
    ) -> std::result::Result<DataChannelRequest, Self::Error> {
        let mut src: &[u8] = src;
        if src.is_empty() {
            return Err(invalid_data("empty data channel frame"));
        }
        match src.get_u8() {
            TAG_CLOSE => Ok(DataChannelRequest::Close),
            TAG_DATA => {
                if src.len() < DATA_HEADER_LEN - 1 {
                    return Err(invalid_data("truncated data channel header"));
                }
                let sequence_number = src.get_u64_le();
                let origin_id = src.get_u64_le();
                let watermark = src.get_u64_le();
                let chunk_number = src.get_u64_le();
                let number_of_tuples = src.get_u64_le();
                let last_chunk = src.get_u8() != 0;
                let data_len = src.get_u32_le() as usize;
                let child_count = src.get_u32_le() as usize;

                if src.len() < data_len {
                    return Err(invalid_data("truncated data channel payload"));
                }
                let data = src[..data_len].to_vec();
                src.advance(data_len);

                // Every child needs at least its 4 byte length, which bounds the allocation by the frame size.
                if child_count > src.len() / 4 {
                    return Err(invalid_data("child buffer count exceeds frame size"));
                }
                let mut child_buffers = Vec::with_capacity(child_count);
                for _ in 0..child_count {
                    if src.len() < 4 {
                        return Err(invalid_data("truncated child buffer header"));
                    }
                    let child_len = src.get_u32_le() as usize;
                    if src.len() < child_len {
                        return Err(invalid_data("truncated child buffer payload"));
                    }
                    child_buffers.push(src[..child_len].to_vec());
                    src.advance(child_len);
                }
                if !src.is_empty() {
                    return Err(invalid_data("trailing bytes after data channel frame"));
                }

                Ok(DataChannelRequest::Data(TupleBuffer {
                    sequence_number,
                    origin_id,
                    watermark,
                    chunk_number,
                    number_of_tuples,
                    last_chunk,
                    data,
                    child_buffers,
                }))
            }
            _ => Err(invalid_data("unknown data channel frame tag")),
        }
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
    RawDataRequestCodec,
>;
pub type DataChannelReceiverReader<R> = Framed<
    FramedRead<R, LengthDelimitedCodec>,
    DataChannelRequest,
    DataChannelRequest,
    RawDataRequestCodec,
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
    let write = tokio_serde::Framed::new(write, RawDataRequestCodec);

    (read, write)
}

pub fn data_channel_receiver<R: AsyncRead + Send + Unpin, W: AsyncWrite + Send + Unpin>(
    stream: Channel<R, W>,
) -> (DataChannelReceiverReader<R>, DataChannelReceiverWriter<W>) {
    let read = FramedRead::new(stream.reader, LengthDelimitedCodec::new());
    let read = tokio_serde::Framed::new(read, RawDataRequestCodec);

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

#[cfg(test)]
mod raw_codec_tests {
    use super::*;

    fn sample(data: Vec<u8>, children: Vec<Vec<u8>>) -> TupleBuffer {
        TupleBuffer {
            sequence_number: 7,
            origin_id: u64::MAX,
            watermark: 42,
            chunk_number: 3,
            number_of_tuples: 100,
            last_chunk: true,
            data,
            child_buffers: children,
        }
    }

    fn roundtrip(item: &DataChannelRequest) -> io::Result<DataChannelRequest> {
        let mut codec = RawDataRequestCodec;
        let bytes = Pin::new(&mut codec).serialize(item)?;
        Pin::new(&mut codec).deserialize(&BytesMut::from(&bytes[..]))
    }

    #[test]
    fn data_roundtrip_with_children() {
        let data: Vec<u8> = (0..4096u32).map(|i| (i * 31) as u8).collect();
        let buffer = sample(data, vec![vec![1, 2, 3], vec![], vec![9; 5000]]);
        let DataChannelRequest::Data(decoded) =
            roundtrip(&DataChannelRequest::Data(buffer.clone())).unwrap()
        else {
            panic!("expected data");
        };
        assert_eq!(decoded, buffer);
    }

    #[test]
    fn empty_data_roundtrip() {
        let buffer = sample(vec![], vec![]);
        let DataChannelRequest::Data(decoded) =
            roundtrip(&DataChannelRequest::Data(buffer.clone())).unwrap()
        else {
            panic!("expected data");
        };
        assert_eq!(decoded, buffer);
    }

    #[test]
    fn close_roundtrip() {
        assert!(matches!(
            roundtrip(&DataChannelRequest::Close).unwrap(),
            DataChannelRequest::Close
        ));
    }

    #[test]
    fn malformed_frames_are_rejected_without_panicking() {
        let mut codec = RawDataRequestCodec;
        let valid = Pin::new(&mut codec)
            .serialize(&DataChannelRequest::Data(sample(
                vec![1, 2, 3, 4],
                vec![vec![5, 6]],
            )))
            .unwrap();
        // every strict prefix of a valid frame is invalid
        for len in 0..valid.len() {
            let result = Pin::new(&mut codec).deserialize(&BytesMut::from(&valid[..len]));
            assert!(result.is_err(), "prefix of length {len} must be rejected");
        }
        // trailing garbage and unknown tags are invalid
        let mut trailing = BytesMut::from(&valid[..]);
        trailing.put_u8(0);
        assert!(Pin::new(&mut codec).deserialize(&trailing).is_err());
        assert!(
            Pin::new(&mut codec)
                .deserialize(&BytesMut::from(&[9u8][..]))
                .is_err()
        );
        // a huge child count in a tiny frame must not allocate
        let mut huge = BytesMut::from(&valid[..DATA_HEADER_LEN - 4]);
        huge.put_u32_le(u32::MAX);
        assert!(Pin::new(&mut codec).deserialize(&huge).is_err());
    }
}
