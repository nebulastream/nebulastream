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
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::InputFormatters
{
/// The RawInputFormatter is NoOp Formatter that just passes along data without modifications
class RawInputFormatter final : public InputFormatter
{
public:
    RawInputFormatter() = default;
    RawInputFormatter(const RawInputFormatter&) = delete;
    RawInputFormatter& operator=(const RawInputFormatter&) = delete;
    RawInputFormatter(RawInputFormatter&&) = delete;
    RawInputFormatter& operator=(RawInputFormatter&&) = delete;

    void parseTupleBufferRaw(
        const NES::Memory::TupleBuffer& tbRaw,
        NES::Memory::AbstractBufferProvider& bufferProvider,
        size_t numBytesInTBRaw,
        const std::function<void(const Memory::TupleBuffer& buffer, bool addBufferMetaData)>& emitFunction) override;

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;
};

}
