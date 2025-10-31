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

#include <Functions/ConcatPhysicalFunction.hpp>

#include <cstdint>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val.hpp>

namespace NES
{

ConcatPhysicalFunction::ConcatPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal ConcatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const nautilus::val<uint32_t> floatSize{4};

    const auto leftValue = leftPhysicalFunction.execute(record, arena).cast<VariableSizedData>();
    const auto leftSize = leftValue.getContentSize();
    const auto leftCount = leftSize / floatSize;

    const auto rightValue = rightPhysicalFunction.execute(record, arena).cast<VariableSizedData>();
    const auto rightSize = rightValue.getContentSize();
    const auto rightCount = rightSize / floatSize;

    const auto newSize = leftSize + rightSize;
    auto newVarSizeData = arena.allocateVariableSizedData(newSize);

    auto rightFloatsToCopyPerOneLeft = rightCount / leftCount;
    nautilus::val<uint32_t> outIdx{0};
    for (nautilus::val<uint32_t> i = 0; i < leftCount; ++i)
    {
        nautilus::memcpy(
            newVarSizeData.getContent() + outIdx * floatSize,
            leftValue.getContent() + i * floatSize,
            floatSize);
        ++outIdx;

        nautilus::memcpy(
            newVarSizeData.getContent() + outIdx * floatSize,
            rightValue.getContent() + i * rightFloatsToCopyPerOneLeft * floatSize,
            rightFloatsToCopyPerOneLeft * floatSize);
        outIdx += rightFloatsToCopyPerOneLeft;
    }

    VarVal(nautilus::val<uint32_t>(newSize)).writeToMemory(newVarSizeData.getReference());
    return newVarSizeData;
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterConcatPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "Concat function must have exactly two sub-functions");
    return ConcatPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctions[1]);
}
}
