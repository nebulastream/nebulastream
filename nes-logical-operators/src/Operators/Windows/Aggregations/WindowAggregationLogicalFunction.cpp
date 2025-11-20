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
#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <fmt/format.h>

namespace NES
{

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(
    DataType inputStamp, DataType partialAggregateStamp, DataType finalAggregateStamp, const FieldAccessLogicalFunction& onField)
    : WindowAggregationLogicalFunction(
          std::move(inputStamp), std::move(partialAggregateStamp), std::move(finalAggregateStamp), onField, onField)
{
}

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(
    DataType inputStamp,
    DataType partialAggregateStamp,
    DataType finalAggregateStamp,
    FieldAccessLogicalFunction onField,
    FieldAccessLogicalFunction asField)
    : inputStamp(std::move(inputStamp))
    , partialAggregateStamp(std::move(partialAggregateStamp))
    , finalAggregateStamp(std::move(finalAggregateStamp))
    , onField(std::move(onField))
    , asField(std::move(asField))
{
}

std::string WindowAggregationLogicalFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

DataType WindowAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

DataType WindowAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

DataType WindowAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

FieldAccessLogicalFunction WindowAggregationLogicalFunction::getOnField() const
{
    return onField;
}

FieldAccessLogicalFunction WindowAggregationLogicalFunction::getAsField() const
{
    return asField;
}

void WindowAggregationLogicalFunction::setInputStamp(DataType inputStamp)
{
    this->inputStamp = std::move(inputStamp);
}

void WindowAggregationLogicalFunction::setPartialAggregateStamp(DataType partialAggregateStamp)
{
    this->partialAggregateStamp = std::move(partialAggregateStamp);
}

void WindowAggregationLogicalFunction::setFinalAggregateStamp(DataType finalAggregateStamp)
{
    this->finalAggregateStamp = std::move(finalAggregateStamp);
}

void WindowAggregationLogicalFunction::setOnField(FieldAccessLogicalFunction onField)
{
    this->onField = std::move(onField);
}

void WindowAggregationLogicalFunction::setAsField(FieldAccessLogicalFunction asField)
{
    this->asField = std::move(asField);
}

bool WindowAggregationLogicalFunction::operator==(
    const std::shared_ptr<WindowAggregationLogicalFunction>& otherWindowAggregationLogicalFunction) const
{
    return this->getName() == otherWindowAggregationLogicalFunction->getName()
        && this->onField == otherWindowAggregationLogicalFunction->onField
        && this->asField == otherWindowAggregationLogicalFunction->asField;
}

}
