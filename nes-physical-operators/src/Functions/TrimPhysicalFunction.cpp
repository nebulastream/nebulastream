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

#include <Functions/TrimPhysicalFunction.hpp>

#include <cstdint>
#include <utility>

#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <nautilus/std/cstring.h>
#include <val.hpp>

namespace NES
{

TrimPhysicalFunction::TrimPhysicalFunction(PhysicalFunction childPhysicalFunction)
    : childPhysicalFunction(std::move(childPhysicalFunction))
{
}

VarVal TrimPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto childValue = childPhysicalFunction.execute(record, arena).cast<VariableSizedData>();
    const auto childSize = childValue.getContentSize();
    const auto childContent = childValue.getContent();

    if (childSize == nautilus::val<uint32_t>(0))
    {
        auto emptyVarSizeData = arena.allocateVariableSizedData(nautilus::val<uint32_t>(0));
        VarVal(nautilus::val<uint32_t>(0)).writeToMemory(emptyVarSizeData.getReference());
        return emptyVarSizeData;
    }

    const auto space = nautilus::val<char>(' ');
    const auto tab = nautilus::val<char>('\t');
    const auto newline = nautilus::val<char>('\n');
    const auto carriageReturn = nautilus::val<char>('\r');

    nautilus::val<uint32_t> start = 0;
    while (start < childSize)
    {
        const auto currentChar = readValueFromMemRef<char>(childContent + start);
        const auto isWhiteSpace = (currentChar == space) || (currentChar == tab) || (currentChar == newline) || (currentChar == carriageReturn);
        if (!isWhiteSpace)
        {
            break;
        }
        ++start;
    }

    auto end = childSize;
    const auto one = nautilus::val<uint32_t>(1);
    while (end > start)
    {
        const auto currentChar = readValueFromMemRef<char>(childContent + (end - one));
        const auto isWhiteSpace = (currentChar == space) || (currentChar == tab) || (currentChar == newline) || (currentChar == carriageReturn);
        if (!isWhiteSpace)
        {
            break;
        }
        --end;
    }

    const auto trimmedSize = end - start;
    auto trimmedVarSizeData = arena.allocateVariableSizedData(trimmedSize);

    if (trimmedSize > nautilus::val<uint32_t>(0))
    {
        nautilus::memcpy(trimmedVarSizeData.getContent(), childContent + start, trimmedSize);
    }

    VarVal(trimmedSize).writeToMemory(trimmedVarSizeData.getReference());
    return trimmedVarSizeData;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTrimPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 1,
                 "Trim function must have exactly one sub-function");
    return TrimPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0]);
}

}