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

#pragma once

#include <snappy.h>
#include <Decoders/Decoder.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
/// Decoder for Snappy encoded data.
/// The buffers of encoded data must arrive sequentially in the right order.
/// The data must have been decompressed in the framed snappy format! Otherwise, we cannot tell the length of the compressed block!

class SnappyDecoder final : public Decoder
{
public:
    explicit SnappyDecoder();
    ~SnappyDecoder() override = default;

    SnappyDecoder(const SnappyDecoder&) = delete;
    SnappyDecoder& operator=(const SnappyDecoder&) = delete;
    SnappyDecoder(SnappyDecoder&&) = delete;
    SnappyDecoder& operator=(SnappyDecoder&&) = delete;

    /// The snappy frame format consists of frames which follow the following format. It is briefly explained here,
    /// since part of the decoding process is hardcoded in this implementation due to a lack of a proper snappy "streaming"
    /// interface.
    ///
    /// A snappy frame starts with a 1 byte identifier (0 = compressed frame, 1 = decompressed frame, -1 = Snappy identifier frame).
    ///
    /// Following are 3 bytes, which give the length of the compressed payload (little endian). This length also includes
    /// the 4 byte checksum, which is prepended to the body of compressed and uncompressed frames. Since snappy's
    /// uncompress function cannot handle this checksum, we skip over it in this implementation, meaning that we only
    /// decode the last length - 4 bytes of the frame.
    ///
    /// For more information about the snappy framing-format, visit https://github.com/google/snappy/blob/main/framing_format.txt
    void decodeAndEmit(
        TupleBuffer& encodedBuffer,
        TupleBuffer& emptyDecodedBuffer,
        const std::function<std::optional<TupleBuffer>(TupleBuffer&, const DecodeStatusType)>& emitAndProvide) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    /// The start of this vector is the start of a Snappy frame. If we have accumulated a whole frame (index 1-3 tells the length of the frame)
    /// we can decode it.
    std::vector<char> encodedBufferStorage;
};
}
