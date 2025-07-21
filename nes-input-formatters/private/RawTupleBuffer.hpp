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
namespace NES::InputFormatters
{
/// Takes a tuple buffer containing raw, unformatted data and wraps it into an object that fulfills the following purposes:
/// 1. The RawTupleBuffer allows its users to operate on string_views, instead of handling raw pointers (which a TupleBuffer would require)
/// 2. It exposes only those functions of the TupleBuffer that are required for formatting
/// 3. It selectively exposes the reduced set of functions, to prohibit setting, e.g., the SequenceNumber in InputFormatIndexer
/// 4. It exposes functions with more descriptive names, e.g., `getNumberOfBytes()` instead of `getNumberOfTuples`
/// 5. The type (RawTupleBuffer) makes it clear that we are dealing with raw data and not with (formatted) tuples
class RawTupleBuffer
{
    Memory::TupleBuffer rawBuffer;
    std::string_view bufferView;

public:
    RawTupleBuffer() = default;
    ~RawTupleBuffer() = default;
    explicit RawTupleBuffer(Memory::TupleBuffer rawTupleBuffer)
        : rawBuffer(std::move(rawTupleBuffer)), bufferView(rawBuffer.getBuffer<const char>(), rawBuffer.getNumberOfTuples()) { };

    RawTupleBuffer(RawTupleBuffer&& other) noexcept = default;
    RawTupleBuffer& operator=(RawTupleBuffer&& other) noexcept = default;
    RawTupleBuffer(const RawTupleBuffer& other) = default;
    RawTupleBuffer& operator=(const RawTupleBuffer& other) = default;

    [[nodiscard]] size_t getNumberOfBytes() const noexcept { return rawBuffer.getNumberOfTuples(); }
    [[nodiscard]] size_t getBufferSize() const noexcept { return rawBuffer.getBufferSize(); }
    [[nodiscard]] SequenceNumber getSequenceNumber() const noexcept { return rawBuffer.getSequenceNumber(); }
    [[nodiscard]] ChunkNumber getChunkNumber() const noexcept { return rawBuffer.getChunkNumber(); }
    [[nodiscard]] OriginId getOriginId() const noexcept { return rawBuffer.getOriginId(); }
    [[nodiscard]] std::string_view getBufferView() const noexcept { return bufferView; }

    [[nodiscard]] uint64_t getNumberOfTuples() const noexcept { return rawBuffer.getNumberOfTuples(); }
    void setNumberOfTuples(const uint64_t numberOfTuples) const noexcept { rawBuffer.setNumberOfTuples(numberOfTuples); }
    /// Allows to emit the underlying buffer without exposing it to the outside
    void emit(PipelineExecutionContext& pec, const PipelineExecutionContext::ContinuationPolicy continuationPolicy) const
    {
        pec.emitBuffer(rawBuffer, continuationPolicy);
    }
    [[nodiscard]] const Memory::TupleBuffer& getRawBuffer() const noexcept { return rawBuffer; }

    void setSpanningTuple(const std::string_view spanningTuple) { this->bufferView = spanningTuple; }
};
}
