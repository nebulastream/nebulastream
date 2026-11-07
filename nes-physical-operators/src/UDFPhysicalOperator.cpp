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

#include <UDFPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <Arena.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <UdfDescriptor.hpp>
#include <UdfInterpreter.hpp>
#include <UdfStage.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Runs in the C++ runtime: stores a runtime pointer into an 8-byte staging slot.
void storeContentPointer(std::int8_t* slot, std::int8_t* contentPointer)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    *reinterpret_cast<std::int8_t**>(slot) = contentPointer;
}

/// Runs in the C++ runtime: loads the pointer that an 8-byte result slot holds.
std::int8_t* loadContentPointer(std::int8_t* slot)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return *reinterpret_cast<std::int8_t**>(slot);
}

/// The stage throws UdfExecutionError on a recoverable failure; the throw unwinds out of nautilus::invoke and fails only the
/// query (the worker survives).
void executeStageRow(
    UdfStage* stage,
    const std::int8_t* argValues,
    const std::int8_t* argLens,
    const std::int8_t* argNulls,
    std::int8_t* results,
    std::int8_t* resultLens,
    std::int8_t* resultNulls)
{
    stage->executeRow(argValues, argLens, argNulls, results, resultLens, resultNulls);
}
}

UDFPhysicalOperator::UDFPhysicalOperator(
    std::vector<Argument> arguments,
    std::vector<Result> results,
    const std::vector<UdfDescriptor>& udfs,
    const UdfInterpreter interpreter,
    const bool fused)
    : arguments(std::move(arguments)), results(std::move(results)), stage(std::make_shared<UdfStage>(udfs, interpreter, fused))
{
}

void UDFPhysicalOperator::setup(ExecutionContext&, CompilationContext& compilationContext) const
{
    for (const auto& argument : arguments)
    {
        argument.function.setup(compilationContext);
    }
}

void UDFPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto& arena = ctx.pipelineMemoryProvider.arena;
    const auto argCount = arguments.size();
    const auto resultCount = results.size();
    const auto argValues = arena.allocateMemory(nautilus::val<size_t>(argCount * UdfStage::SLOT_BYTES));
    const auto argLens = arena.allocateMemory(nautilus::val<size_t>(argCount * UdfStage::SLOT_BYTES));
    const auto argNulls = arena.allocateMemory(nautilus::val<size_t>(argCount));
    const auto resultSlots = arena.allocateMemory(nautilus::val<size_t>(resultCount * UdfStage::SLOT_BYTES));
    const auto resultLens = arena.allocateMemory(nautilus::val<size_t>(resultCount * UdfStage::SLOT_BYTES));
    const auto resultNulls = arena.allocateMemory(nautilus::val<size_t>(resultCount));

    for (nautilus::static_val<size_t> i = 0; i < argCount; ++i)
    {
        const auto& argument = arguments.at(nautilus::static_val<int>(i));
        const auto value = argument.function.execute(record, arena);
        const auto isNull = value.isNullable() ? value.isNull() : nautilus::val<bool>(false);
        VarVal{isNull}.writeToMemory(argNulls + nautilus::val<uint64_t>(i));

        const auto valueSlot = argValues + nautilus::val<uint64_t>(i * UdfStage::SLOT_BYTES);
        const auto lenSlot = argLens + nautilus::val<uint64_t>(i * UdfStage::SLOT_BYTES);
        if (argument.declaredType.type == DataType::Type::VARSIZED)
        {
            const auto varSized = value.getRawValueAs<VariableSizedData>();
            nautilus::invoke(storeContentPointer, valueSlot, varSized.getContent());
            VarVal{varSized.getSize()}.writeToMemory(lenSlot);
        }
        else
        {
            value.writeToMemory(valueSlot);
            VarVal{nautilus::val<uint64_t>(0)}.writeToMemory(lenSlot);
        }
    }

    nautilus::invoke(
        executeStageRow, nautilus::val<UdfStage*>(stage.get()), argValues, argLens, argNulls, resultSlots, resultLens, resultNulls);

    const DataType boolType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE};
    const DataType lengthType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE};
    for (nautilus::static_val<size_t> i = 0; i < resultCount; ++i)
    {
        const auto& result = results.at(nautilus::static_val<int>(i));
        const auto isNull = VarVal::readNonNullableVarValFromMemory(resultNulls + nautilus::val<uint64_t>(i), boolType)
                                .getRawValueAs<nautilus::val<bool>>();
        if (result.type.type == DataType::Type::VARSIZED)
        {
            /// The stage's copy of the result lives only until its next row, so it moves into the arena, at its exact length.
            const auto length
                = VarVal::readNonNullableVarValFromMemory(resultLens + nautilus::val<uint64_t>(i * UdfStage::SLOT_BYTES), lengthType)
                      .getRawValueAs<nautilus::val<uint64_t>>();
            const auto content = nautilus::invoke(loadContentPointer, resultSlots + nautilus::val<uint64_t>(i * UdfStage::SLOT_BYTES));
            const auto buffer = arena.allocateVariableSizedData(length);
            nautilus::memcpy(buffer.getContent(), content, length);
            record.write(result.field, VarVal{buffer, true, isNull});
        }
        else
        {
            record.write(
                result.field,
                VarVal::readVarValFromMemory(resultSlots + nautilus::val<uint64_t>(i * UdfStage::SLOT_BYTES), result.type, isNull));
        }
    }
    executeChild(ctx, record);
}

std::optional<PhysicalOperator> UDFPhysicalOperator::getChild() const
{
    return child;
}

void UDFPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
