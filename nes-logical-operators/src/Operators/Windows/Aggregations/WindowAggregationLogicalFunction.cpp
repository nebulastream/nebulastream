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
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <fmt/format.h>

namespace NES
{

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(const FieldAccessLogicalFunction& onField)
    : WindowAggregationLogicalFunction(onField, onField)
{
}

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(FieldAccessLogicalFunction onField, FieldAccessLogicalFunction asField)
    : onField(std::move(onField)), asField(std::move(asField))
{
}

std::string WindowAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

FieldAccessLogicalFunction WindowAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction WindowAggregationLogicalFunction::getAsField() const
{
    return asField;
}

void WindowAggregationLogicalFunction::setOnField(FieldAccessLogicalFunction onField)
{
    this->onField = std::move(onField);
}

void WindowAggregationLogicalFunction::setAsField(FieldAccessLogicalFunction asField)
{
    this->asField = std::move(asField);
}

void WindowAggregationLogicalFunction::setDataTypeOnField(const DataType newDataType)
{
    setOnField(getOnField().withDataType(newDataType).get<FieldAccessLogicalFunction>());
}

void WindowAggregationLogicalFunction::setFieldNameOnField(const std::string_view newFieldName)
{
    setOnField(getOnField().withFieldName(newFieldName).get<FieldAccessLogicalFunction>());
}

void WindowAggregationLogicalFunction::setDataTypeAsField(const DataType newDataType)
{
    setAsField(getAsField().withDataType(newDataType).get<FieldAccessLogicalFunction>());
}

void WindowAggregationLogicalFunction::setFieldNameAsField(const std::string_view newFieldName)
{
    setAsField(getAsField().withFieldName(newFieldName).get<FieldAccessLogicalFunction>());
}

bool WindowAggregationLogicalFunction::operator==(
    const std::shared_ptr<WindowAggregationLogicalFunction>& otherWindowAggregationLogicalFunction) const
{
    return this->getName() == otherWindowAggregationLogicalFunction->getName()
        && this->onField == otherWindowAggregationLogicalFunction->onField
        && this->asField == otherWindowAggregationLogicalFunction->asField;
}

}
