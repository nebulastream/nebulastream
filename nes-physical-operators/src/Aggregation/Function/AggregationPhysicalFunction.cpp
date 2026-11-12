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

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>

#include <cstdint>
#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

AggregationPhysicalFunction::AggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : inputType(std::move(inputType))
    , resultType(std::move(resultType))
    , inputFunction(std::move(inputFunction))
    , resultFieldIdentifier(std::move(resultFieldIdentifier))
{
}

AggregationPhysicalFunction::AggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    PhysicalScalarFunctions scalarFunctions)
    : inputType(std::move(inputType))
    , resultType(std::move(resultType))
    , inputFunction(std::move(inputFunction))
    , resultFieldIdentifier(std::move(resultFieldIdentifier))
    , scalarFunctions(std::move(scalarFunctions))
{
}

VarVal AggregationPhysicalFunction::applyScalarFunction(
    const std::string_view role, const std::initializer_list<ScalarOperand> operands, ArenaRef& arena) const
{
    const auto scalarFunction = scalarFunctions.find(std::string{role});
    PRECONDITION(
        scalarFunction != scalarFunctions.end(),
        "Aggregation function has no scalar function for role '{}'. The logical aggregation function must provide it.",
        role);
    /// The record only exists at trace time: the operand names are resolved during compilation, not per tuple.
    Record operandRecord;
    for (const auto& [operandName, operandValue] : operands)
    {
        operandRecord.write(QualifiedIdentifier::parse(operandName), operandValue);
    }
    return scalarFunction->second.execute(operandRecord, arena);
}

void AggregationPhysicalFunction::storeNull(const nautilus::val<AggregationState*>& aggregationState, const nautilus::val<bool>& isNull)
{
    const auto memAreaNull = static_cast<nautilus::val<int8_t*>>(aggregationState);
    VarVal{isNull}.writeToMemory(memAreaNull);
}

nautilus::val<bool> AggregationPhysicalFunction::readNull(const nautilus::val<AggregationState*>& aggregationState)
{
    const auto memAreaNull = static_cast<nautilus::val<int8_t*>>(aggregationState);
    auto isNull = readValueFromMemRef<bool>(memAreaNull);
    return isNull;
}

AggregationPhysicalFunction::~AggregationPhysicalFunction() = default;
}
