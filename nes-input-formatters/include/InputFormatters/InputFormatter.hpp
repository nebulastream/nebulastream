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

#include <cstddef>
#include <memory>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::InputFormatters
{
/// Forward referencing SequenceShredder to keep it as a private implementation detail of nes-input-formatters
class SequenceShredder;

class InputFormatter
{
public:
    InputFormatter() = default;
    virtual ~InputFormatter() = default;

    InputFormatter(const InputFormatter&) = default;
    InputFormatter& operator=(const InputFormatter&) = delete;
    InputFormatter(InputFormatter&&) = delete;
    InputFormatter& operator=(InputFormatter&&) = delete;

    /// Must only manipulate the sequenceShredder and otherwise be free of side-effects
    virtual void parseTupleBufferRaw(
        const NES::Memory::TupleBuffer& rawTB,
        NES::PipelineExecutionContext& pipelineExecutionContext,
        size_t numBytesInRawTB,
        SequenceShredder& sequenceShredder)
        = 0;

    /// Since there is no symbol that represents EoF/EoS, the final buffer does not end in a tuple delimiter. We need to 'manually' flush
    /// the final tuple, between the last tuple delimiter of the final buffer and (including) the last byte of the final buffer.
    virtual void
    flushFinalTuple(OriginId originId, NES::PipelineExecutionContext& pipelineExecutionContext, SequenceShredder& sequenceShredder)
        = 0;

    virtual size_t getSizeOfTupleDelimiter() = 0;
    virtual size_t getSizeOfFieldDelimiter() = 0;

    friend std::ostream& operator<<(std::ostream& os, const InputFormatter& inputFormatter) { return inputFormatter.toString(os); }

protected:
    virtual std::ostream& toString(std::ostream& os) const = 0;
};
}
