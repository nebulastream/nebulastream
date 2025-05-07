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

#include <memory>
#include <sstream>
#include <string>
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <fmt/format.h>
#include <LogicalOperators/Windows/Aggregations/WindowAggregationFunction.hpp>

namespace NES::Logical
{

WindowAggregationFunction::WindowAggregationFunction(
    std::shared_ptr<DataType> inputStamp,
    std::shared_ptr<DataType> partialAggregateStamp,
    std::shared_ptr<DataType> finalAggregateStamp,
    FieldAccessFunction onField)
    : WindowAggregationFunction(inputStamp, partialAggregateStamp, finalAggregateStamp, onField, onField)
{
}

WindowAggregationFunction::WindowAggregationFunction(
    std::shared_ptr<DataType> inputStamp,
    std::shared_ptr<DataType> partialAggregateStamp,
    std::shared_ptr<DataType> finalAggregateStamp,
    FieldAccessFunction onField,
    FieldAccessFunction asField)
    : inputStamp(std::move(inputStamp))
    , partialAggregateStamp(std::move(partialAggregateStamp))
    , finalAggregateStamp(std::move(finalAggregateStamp))
    , onField(onField)
    , asField(asField)
{
}

std::string WindowAggregationFunction::toString() const
{
    return fmt::format("WindowAggregation: onField={} asField={}", onField, asField);
}

std::shared_ptr<DataType> WindowAggregationFunction::getInputStamp() const
{
    return inputStamp;
}

std::shared_ptr<DataType> WindowAggregationFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

std::shared_ptr<DataType> WindowAggregationFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

bool WindowAggregationFunction::operator==(
    std::shared_ptr<WindowAggregationFunction> otherWindowAggregationFunction) const
{
    return this->getName() == otherWindowAggregationFunction->getName()
        && this->onField == otherWindowAggregationFunction->onField
        && this->asField == otherWindowAggregationFunction->asField;
}

}
