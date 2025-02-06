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


#include <iomanip>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/ostream.h>

namespace NES::Nautilus
{

VariableSizedData::VariableSizedData(ScratchMemory memory) : memory(memory)
{
}

nautilus::val<bool> operator==(const VariableSizedData& lhs, const VariableSizedData& rhs)
{
    return lhs.memory == rhs.memory;
}

[[nodiscard]] nautilus::val<size_t> VariableSizedData::getSize() const
{
    return memory.size;
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getContent() const
{
    return memory.data;
}

[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const VariableSizedData& variableSizedData)
{
    oss << "Size(" << variableSizedData.memory.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < variableSizedData.memory.size; ++i)
    {
        const nautilus::val<int> byte = Util::readValueFromMemRef<int8_t>((variableSizedData.getContent() + i)) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}
}
