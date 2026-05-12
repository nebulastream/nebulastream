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

#include <Functions/CharLengthPhysicalFunction.hpp>

#include <cstdint>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <select.hpp>
#include <val_arith.hpp>

namespace NES
{

CharLengthPhysicalFunction::CharLengthPhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
}

VarVal CharLengthPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto inputValue = childPhysicalFunction.execute(record, arena);
    const auto variableSizedData = inputValue.getRawValueAs<VariableSizedData>();
    const auto ptr = variableSizedData.getContent();
    const auto size = variableSizedData.getSize();

    /// Count UTF-8 codepoints: every byte except continuation bytes (0b10xxxxxx) starts a new codepoint.
    /// Branchless via nautilus::select so the JIT can vectorize the loop.
    nautilus::val<uint64_t> count = 0;
    for (nautilus::val<uint64_t> i = 0; i < size; i = i + nautilus::val<uint64_t>(1))
    {
        const nautilus::val<int8_t> byte = *(ptr + i);
        const auto isLeading
            = (byte & nautilus::val<int8_t>(static_cast<int8_t>(0xC0))) != nautilus::val<int8_t>(static_cast<int8_t>(0x80));
        count = count + nautilus::select(isLeading, nautilus::val<uint64_t>(1), nautilus::val<uint64_t>(0));
    }

    return VarVal{count, inputValue.isNullable(), inputValue.isNull()};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterCHAR_LENGTHPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1, "CHAR_LENGTH function must have exactly one child function");
    return CharLengthPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}
}
