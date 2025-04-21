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
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES
{

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(
    std::shared_ptr<DataType> inputStamp,
    std::shared_ptr<DataType> partialAggregateStamp,
    std::shared_ptr<DataType> finalAggregateStamp,
    FieldAccessLogicalFunction onField)
    : inputStamp(std::move(inputStamp))
    , partialAggregateStamp(std::move(partialAggregateStamp))
    , finalAggregateStamp(std::move(finalAggregateStamp))
    , onField(onField)
    , asField(onField)
{
}

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(
    std::shared_ptr<DataType> inputStamp,
    std::shared_ptr<DataType> partialAggregateStamp,
    std::shared_ptr<DataType> finalAggregateStamp,
    FieldAccessLogicalFunction onField,
    FieldAccessLogicalFunction asField)
    : inputStamp(std::move(inputStamp))
    , partialAggregateStamp(std::move(partialAggregateStamp))
    , finalAggregateStamp(std::move(finalAggregateStamp))
    , onField(std::move(onField))
    , asField(asField)
{
}

std::string WindowAggregationLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "WindowAggregation: ";
    ss << " onField=" << onField;
    ss << " asField=" << asField;
    ss << std::endl;
    return ss.str();
}

std::shared_ptr<DataType> WindowAggregationLogicalFunction::getInputStamp() const
{
    return inputStamp;
}

std::shared_ptr<DataType> WindowAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return partialAggregateStamp;
}

std::shared_ptr<DataType> WindowAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return finalAggregateStamp;
}

bool WindowAggregationLogicalFunction::operator==(
    std::shared_ptr<WindowAggregationLogicalFunction> otherWindowAggregationLogicalFunction) const
{
    return this->getName() == otherWindowAggregationLogicalFunction->getName()
        && this->onField == otherWindowAggregationLogicalFunction->onField
        && this->asField == otherWindowAggregationLogicalFunction->asField;
}

}
