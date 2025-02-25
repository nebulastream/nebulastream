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
#include <Operators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>

namespace NES::Windowing
{

WindowAggregationFunction::WindowAggregationFunction(std::unique_ptr<DataType> inputStamp, std::unique_ptr<DataType> partialAggregateStamp, std::unique_ptr<DataType> finalAggregateStamp, std::unique_ptr<FieldAccessLogicalFunction> onField)
    :  inputStamp(std::move(inputStamp)), partialAggregateStamp(std::move(partialAggregateStamp)), finalAggregateStamp(std::move(finalAggregateStamp)), onField(onField->clone()), asField(std::move(onField))
{
}

WindowAggregationFunction::WindowAggregationFunction(
    std::unique_ptr<DataType> inputStamp, std::unique_ptr<DataType> partialAggregateStamp, std::unique_ptr<DataType> finalAggregateStamp, std::unique_ptr<LogicalFunction> onField, std::unique_ptr<LogicalFunction> asField)
    : inputStamp(std::move(inputStamp)), partialAggregateStamp(std::move(partialAggregateStamp)), finalAggregateStamp(std::move(finalAggregateStamp)),onField(std::move(onField)), asField(std::move(asField))
{
}

std::unique_ptr<WindowAggregationFunction> WindowAggregationFunction::as(std::unique_ptr<LogicalFunction> asField)
{
    this->asField = Util::unique_ptr_dynamic_cast<FieldAccessLogicalFunction>(std::move(asField));
    return std::unique_ptr<WindowAggregationFunction>(this);
}

LogicalFunction& WindowAggregationFunction::as() const
{
    if (asField == nullptr)
    {
        return *onField;
    }
    return *asField;
}

std::string WindowAggregationFunction::toString() const
{
    std::stringstream ss;
    ss << "WindowAggregation: ";
    ss << " Type=" << getTypeAsString();
    ss << " onField=" << *onField;
    ss << " asField=" << *asField;
    ss << std::endl;
    return ss.str();
}

WindowAggregationFunction::Type WindowAggregationFunction::getType() const
{
    return aggregationType;
}

std::string WindowAggregationFunction::getTypeAsString() const
{
    return std::string(magic_enum::enum_name(aggregationType));
}

LogicalFunction& WindowAggregationFunction::on() const
{
    return *onField;
}

DataType& WindowAggregationFunction::getInputStamp() const
{
    return *inputStamp;
}

DataType& WindowAggregationFunction::getPartialAggregateStamp() const
{
    return *partialAggregateStamp;
}

DataType& WindowAggregationFunction::getFinalAggregateStamp() const
{
    return *finalAggregateStamp;
}

bool WindowAggregationFunction::operator==(std::shared_ptr<WindowAggregationFunction> otherWindowAggregationFunction) const
{
    return this->getType() == otherWindowAggregationFunction->getType() && this->onField == otherWindowAggregationFunction->onField
        && this->asField == otherWindowAggregationFunction->asField;
}

}
