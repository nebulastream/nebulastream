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

#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <utility>

#include "DataTypes/DataTypeProvider.hpp"

#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <fmt/format.h>
#include "Functions/LogicalFunction.hpp"
#include "Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp"

namespace NES
{

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(DataType aggregateType, LogicalFunction inputFunction)
    : aggregateType(std::move(aggregateType)), inputFunction(std::move(inputFunction))
{
}
WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(LogicalFunction inputFunction) : inputFunction(std::move(inputFunction))
{
    aggregateType = this->inputFunction.getDataType();
}

LogicalFunction WindowAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string WindowAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: input={}", inputFunction);
}

DataType WindowAggregationLogicalFunction::getAggregateType() const
{
    return aggregateType;
}

bool WindowAggregationLogicalFunction::operator==(
    const std::shared_ptr<WindowAggregationLogicalFunction>& otherWindowAggregationLogicalFunction) const
{
    return this->getName() == otherWindowAggregationLogicalFunction->getName()
        && this->inputFunction == otherWindowAggregationLogicalFunction->inputFunction;
}

}
