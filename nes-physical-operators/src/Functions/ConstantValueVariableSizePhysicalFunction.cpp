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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
namespace NES
{
ConstantValueVariableSizePhysicalFunction::ConstantValueVariableSizePhysicalFunction(const int8_t* value, const size_t size)
    : data(size + sizeof(uint32_t))
{
    /// We are copying the data into the function's memory. This is necessary as the data might be destroyed after the function is created.
    /// In the constructor, we have allocated the memory for the data via the make_unique<>(). Thus, we have to copy here the
    /// data into the function's memory. As we expect the first four bytes to be the size of the data, we use a bit_cast to set the size.
    /// Afterward, we copy the value into the data.
    *std::bit_cast<uint32_t*>(data.data()) = size;
    std::memcpy(data.data() + sizeof(uint32_t), value, size);
}

VarVal ConstantValueVariableSizePhysicalFunction::execute(const Record&, ArenaRef&) const
{
    VariableSizedData result(const_cast<int8_t*>(data.data()));
    return result;
}

}
