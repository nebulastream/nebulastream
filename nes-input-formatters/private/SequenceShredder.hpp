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

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string_view>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Formatter.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceRingBuffer.hpp>

#include <ErrorHandling.hpp>

namespace NES::InputFormatters
{

class SequenceShredder
{
    static constexpr size_t SIZE_OF_RING_BUFFER = 1024;

public:
    explicit SequenceShredder() = default;
    ~SequenceShredder();

    /// Thread-safely checks if the buffer represented by the sequence number completes spanning tuples.
    /// Returns a sequence of tuple buffers that represent either 0, 1 or 2 SpanningTuples.
    /// For details on the inner workings of this function, read the description of the class above.
    template <bool HasTupleDelimiter>
    SequenceShredderResult processSequenceNumber(StagedBuffer indexedRawBuffer, SequenceNumberType sequenceNumber)
    requires(HasTupleDelimiter);

    template <bool HasTupleDelimiter>
    SequenceShredderResult processSequenceNumber(StagedBuffer indexedRawBuffer, SequenceNumberType sequenceNumber)
    requires(not HasTupleDelimiter);

    friend std::ostream& operator<<(std::ostream& os, const SequenceShredder& sequenceShredder);

private:
    // Todo: make 'RingBuffer' its own class (in its own file) <-- SequenceShredder should be thin wrapper around RingBuffer (if possible, with a single function)
    SequenceRingBuffer<SIZE_OF_RING_BUFFER> ringBuffer;
};

}

FMT_OSTREAM(NES::InputFormatters::SequenceShredder);
