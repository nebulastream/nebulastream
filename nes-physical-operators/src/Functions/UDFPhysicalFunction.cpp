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

#include <Functions/UDFPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <Arena.hpp>
#include <UdfBackend.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// Each argument occupies a fixed 8-byte slot: a scalar value, or (for a VARSIZED argument) a pointer
/// to the content bytes whose length lives in the parallel `argLens` buffer.
constexpr std::size_t SLOT_BYTES = 8;

/// v1 cap on a VARSIZED result: the arena buffer is pre-allocated at this size (mirrors ToBase64's
/// bounded-output allocation). A larger result fails the query rather than corrupting memory.
constexpr std::uint64_t MAX_VARSIZED_RESULT_BYTES = 8192;

/// Runs in the C++ runtime: stores a runtime pointer into an 8-byte staging slot.
void storeContentPointer(std::int8_t* slot, std::int8_t* contentPointer)
{
    *reinterpret_cast<std::int8_t**>(slot) = contentPointer;
}

/// The backend's execute_*Row throws UdfExecutionError on a recoverable failure; the throw unwinds out
/// of nautilus::invoke and fails only the query (the worker survives).
std::int8_t udfExecuteScalar(
    UdfBackend* backend, const std::int8_t* argValues, const std::int8_t* argLens, const std::int8_t* argNulls, std::int8_t* resultScalar)
{
    int resultNull = 0;
    backend->executeScalarRow(argValues, argLens, argNulls, resultScalar, &resultNull);
    return static_cast<std::int8_t>(resultNull);
}

std::uint64_t udfExecuteVarsized(
    UdfBackend* backend,
    const std::int8_t* argValues,
    const std::int8_t* argLens,
    const std::int8_t* argNulls,
    std::int8_t* resultBuffer,
    std::uint64_t maxResultLen,
    std::int8_t* resultNullOut)
{
    int resultNull = 0;
    const std::uint64_t length = backend->executeVarsizedRow(argValues, argLens, argNulls, resultBuffer, maxResultLen, &resultNull);
    *resultNullOut = static_cast<std::int8_t>(resultNull);
    return length;
}
}

UDFPhysicalFunction::UDFPhysicalFunction(
    std::vector<PhysicalFunction> arguments, std::vector<DataType> argumentTypes, DataType returnType, std::shared_ptr<UdfBackend> backend)
    : arguments(std::move(arguments))
    , argumentTypes(std::move(argumentTypes))
    , returnType(std::move(returnType))
    , backend(std::move(backend))
{
}

VarVal UDFPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto argCount = arguments.size();
    const auto argValues = arena.allocateMemory(nautilus::val<size_t>(argCount * SLOT_BYTES));
    const auto argLens = arena.allocateMemory(nautilus::val<size_t>(argCount * SLOT_BYTES));
    const auto argNulls = arena.allocateMemory(nautilus::val<size_t>(argCount));

    auto anyArgNull = nautilus::val<bool>(false);
    for (nautilus::static_val<size_t> i = 0; i < argCount; ++i)
    {
        const auto value = arguments.at(nautilus::static_val<int>(i)).execute(record, arena);
        const auto isNull = value.isNullable() ? value.isNull() : nautilus::val<bool>(false);
        VarVal{isNull}.writeToMemory(argNulls + nautilus::val<uint64_t>(i));
        anyArgNull = anyArgNull or isNull;

        const auto valueSlot = argValues + nautilus::val<uint64_t>(i * SLOT_BYTES);
        const auto lenSlot = argLens + nautilus::val<uint64_t>(i * SLOT_BYTES);
        if (argumentTypes.at(nautilus::static_val<int>(i)).type == DataType::Type::VARSIZED)
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

    /// Strict-UDF semantics (RETURNS NULL ON NULL INPUT): if any argument is NULL, the result is NULL and
    /// the UDF is not invoked. This spares every UDF author from handling None and keeps a UDF like
    /// currency.add (which would raise on None) well-defined on NULL input. The per-argument null plumbing
    /// above is retained so the ABI stays capable of a future non-strict (CALLED ON NULL INPUT) mode.
    if (anyArgNull)
    {
        if (returnType.type == DataType::Type::VARSIZED)
        {
            return VarVal{VariableSizedData{nullptr, 0}, true, true};
        }
        const auto nullResult = arena.allocateMemory(nautilus::val<size_t>(SLOT_BYTES));
        return VarVal::readVarValFromMemory(nullResult, returnType, nautilus::val<bool>(true));
    }

    const auto backendPtr = nautilus::val<UdfBackend*>(backend.get());

    if (returnType.type == DataType::Type::VARSIZED)
    {
        const auto output = arena.allocateVariableSizedData(nautilus::val<uint64_t>(MAX_VARSIZED_RESULT_BYTES));
        const auto resultNullSlot = arena.allocateMemory(nautilus::val<size_t>(1));
        const auto length = nautilus::invoke(
            udfExecuteVarsized,
            backendPtr,
            argValues,
            argLens,
            argNulls,
            output.getContent(),
            nautilus::val<uint64_t>(MAX_VARSIZED_RESULT_BYTES),
            resultNullSlot);
        const DataType boolType{DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE};
        const auto isNull = VarVal::readNonNullableVarValFromMemory(resultNullSlot, boolType).getRawValueAs<nautilus::val<bool>>();
        return VarVal{VariableSizedData(output.getContent(), length), true, isNull};
    }

    const auto resultScalar = arena.allocateMemory(nautilus::val<size_t>(SLOT_BYTES));
    const auto resultNull = nautilus::invoke(udfExecuteScalar, backendPtr, argValues, argLens, argNulls, resultScalar);
    return VarVal::readVarValFromMemory(resultScalar, returnType, resultNull != nautilus::val<std::int8_t>(0));
}

}
