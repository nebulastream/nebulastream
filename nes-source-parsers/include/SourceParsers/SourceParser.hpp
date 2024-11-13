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

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::SourceParsers
{

/// Takes tuple buffers with raw bytes (TBRaw/TBR), parses the TBRs and writes the formatted data to formatted tuple buffers (TBFormatted/TBF)
class SourceParser
{
public:
    SourceParser() = default;
    virtual ~SourceParser() = default;

    virtual bool parseTupleBufferRaw(
        const NES::Memory::TupleBuffer& tbRaw,
        NES::Memory::AbstractBufferProvider& bufferManager,
        const size_t numBytesInTBRaw,
        const std::function<void(Memory::TupleBuffer& buffer, bool addBufferMetaData)>& emitFunction)
        = 0;

    friend std::ostream& operator<<(std::ostream& out, const SourceParser& sourceParser) { return sourceParser.toString(out); }

protected:
    /// Implemented by children of SourceParser. Called by '<<'. Allows to use '<<' on abstract SourceParser.
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}
