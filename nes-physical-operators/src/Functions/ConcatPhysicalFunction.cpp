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
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/std/cstring.h>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <select.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

namespace NES
{

ConcatPhysicalFunction::ConcatPhysicalFunction(PhysicalFunction leftPhysicalFunction, PhysicalFunction rightPhysicalFunction)
    : leftPhysicalFunction(std::move(leftPhysicalFunction)), rightPhysicalFunction(std::move(rightPhysicalFunction))
{
}

VarVal ConcatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto leftValue = leftPhysicalFunction.execute(record, arena);
    const auto rightValue = rightPhysicalFunction.execute(record, arena);
    const auto leftVariableSizedData = leftValue.cast<VariableSizedData>();
    const auto rightVariableSizedData = rightValue.cast<VariableSizedData>();

    const auto newIsNullable
        = (leftValue.isNullable() or rightValue.isNullable()) ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    const auto condition = (leftValue.isNullable() or rightValue.isNullable()) and (leftValue.isNull() or rightValue.isNull());
    const auto newSize = nautilus::select(
        condition, nautilus::val<uint32_t>{0}, leftVariableSizedData.getContentSize() + rightVariableSizedData.getContentSize());
    const auto newIsNull = nautilus::select(condition, nautilus::val<bool>{true}, nautilus::val<bool>{false});
    const auto newVarSizeData = arena.allocateVariableSizedData(newSize);

    /// Writing the left value and then the right value to the new variable sized data
    nautilus::memcpy(newVarSizeData.getContent(), leftVariableSizedData.getContent(), leftVariableSizedData.getContentSize());
    nautilus::memcpy(
        newVarSizeData.getContent() + leftVariableSizedData.getContentSize(),
        rightVariableSizedData.getContent(),
        rightVariableSizedData.getContentSize());
    VarVal{newSize}.writeToMemory(newVarSizeData.getReference());
    return VarVal{newVarSizeData, newIsNullable, newIsNull};
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterConcatPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 2, "Concat function must have exactly two child functions");
    return ConcatPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0], physicalFunctionRegistryArguments.childFunctions[1]);
}
}
