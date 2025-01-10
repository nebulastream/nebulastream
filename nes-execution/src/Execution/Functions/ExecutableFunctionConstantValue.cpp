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

#include <bit>
#include <Execution/Functions/ExecutableFunctionConstantValue.hpp>
#include <nautilus/function.hpp>

namespace NES::Runtime::Execution::Functions
{

ExecutableFunctionConstantText::ExecutableFunctionConstantText(std::string_view value)
    : sizeIncludingLength(value.size() + sizeof(uint32_t)), data(std::make_unique<int8_t[]>(sizeIncludingLength))
{
    *std::bit_cast<uint32_t*>(data.get()) = value.size();
    memcpy(data.get() + sizeof(uint32_t), value.data(), value.size());
}

VarVal ExecutableFunctionConstantText::execute(const Record&, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) const
{
    auto buffer = nautilus::invoke(
        +[](Memory::AbstractBufferProvider* bufferProvider, size_t sizeIncludingLength, int8_t* data)
        {
            auto textBuffer = bufferProvider->getUnpooledBuffer(sizeIncludingLength).value();
            memcpy(textBuffer.getBuffer(), data, sizeIncludingLength);
            textBuffer.retain();
            return textBuffer.getBuffer();
        },
        bufferProvider,
        nautilus::val<size_t>(sizeIncludingLength),
        nautilus::val<int8_t*>(data.get()));
    return VariableSizedData(buffer);
}

}
