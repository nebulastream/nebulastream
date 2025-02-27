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
#include <functional>
#include <ostream>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>

namespace NES::InputFormatters
{

/// Takes tuple buffers with raw bytes (TBRaw/TBR), parses the TBRs and writes the formatted data to formatted tuple buffers (TBFormatted/TBF)
class InputFormatter
{
public:
    InputFormatter() = default;
    virtual ~InputFormatter() = default;

    InputFormatter(const InputFormatter&) = delete;
    InputFormatter& operator=(const InputFormatter&) = delete;
    InputFormatter(InputFormatter&&) = delete;
    InputFormatter& operator=(InputFormatter&&) = delete;

    virtual void parseTupleBufferRaw(
        const NES::Memory::TupleBuffer& tbRaw,
        NES::Memory::AbstractBufferProvider& bufferProvider,
        size_t numBytesInTBRaw,
        const std::function<void(Memory::TupleBuffer& buffer)>& emitFunction)
        = 0;

    friend std::ostream& operator<<(std::ostream& out, const InputFormatter& inputFormatter) { return inputFormatter.toString(out); }

protected:
    [[nodiscard]] virtual std::ostream& toString(std::ostream& str) const = 0;
};

}

namespace fmt
{
template <>
struct formatter<NES::InputFormatters::InputFormatter> : ostream_formatter
{
};
}
