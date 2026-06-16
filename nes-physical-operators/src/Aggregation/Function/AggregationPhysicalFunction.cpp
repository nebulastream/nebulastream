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

#include <cstddef>
#include <cstdint>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <val_arith.hpp>
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

AggregationPhysicalFunction::~AggregationPhysicalFunction() = default;

AggregationStateRef::AggregationStateRef(nautilus::val<AggregationState*> state, NautilusBuffer& buffer)
    : base(static_cast<nautilus::val<int8_t*>>(state)), parentBuffer(&buffer)
{
}

nautilus::val<int8_t*> AggregationStateRef::data() const
{
    return base;
}

nautilus::val<int8_t*> AggregationStateRef::valueData(const bool hasNullByte) const
{
    /// hasNullByte is a compile-time bool, so we branch here rather than emit a traced pointer-add-by-zero for the non-nullable case.
    return hasNullByte ? data() + nautilus::val<uint64_t>{1} : data();
}

NautilusBuffer& AggregationStateRef::getBuffer() const
{
    return *parentBuffer;
}

nautilus::val<bool> AggregationStateRef::readNull() const
{
    return readValueFromMemRef<bool>(data());
}

void AggregationStateRef::storeNull(const nautilus::val<bool>& isNull) const
{
    VarVal{isNull}.writeToMemory(data());
}
}
