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

#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <InputFormatterRegistry.hpp>
#include <RawInputFormatter.hpp>

namespace NES::InputFormatters
{

std::unique_ptr<InputFormatterRegistryReturnType>
InputFormatterGeneratedRegistrar::RegisterRawInputFormatter(InputFormatterRegistryArguments)
{
    return std::make_unique<RawInputFormatter>();
}

void RawInputFormatter::parseTupleBufferRaw(
    const Memory::TupleBuffer& tbRaw,
    Memory::AbstractBufferProvider&,
    size_t,
    const std::function<void(const Memory::TupleBuffer& buffer, bool addBufferMetaData)>& emitFunction)
{
    emitFunction(tbRaw, true);
}
std::ostream& RawInputFormatter::toString(std::ostream& str) const
{
    return str << "RawInputFormatter";
}
}
