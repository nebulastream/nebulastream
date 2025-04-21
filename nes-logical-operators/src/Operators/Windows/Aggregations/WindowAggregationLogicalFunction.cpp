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
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <magic_enum.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Util/Common.hpp>

namespace NES
{

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(std::unique_ptr<DataType> inputStamp, std::unique_ptr<DataType> partialAggregateStamp, std::unique_ptr<DataType> finalAggregateStamp, std::unique_ptr<FieldAccessLogicalFunction> onField)
    :  inputStamp(std::move(inputStamp)), partialAggregateStamp(std::move(partialAggregateStamp)), finalAggregateStamp(std::move(finalAggregateStamp)), onField(onField->clone()), asField(std::move(onField))
{
}

WindowAggregationLogicalFunction::WindowAggregationLogicalFunction(
    std::unique_ptr<DataType> inputStamp, std::unique_ptr<DataType> partialAggregateStamp, std::unique_ptr<DataType> finalAggregateStamp, std::unique_ptr<LogicalFunction> onField, std::unique_ptr<LogicalFunction> asField)
    : inputStamp(std::move(inputStamp)), partialAggregateStamp(std::move(partialAggregateStamp)), finalAggregateStamp(std::move(finalAggregateStamp)),onField(std::move(onField)), asField(std::move(asField))
{
}

std::unique_ptr<WindowAggregationLogicalFunction> WindowAggregationLogicalFunction::as(std::unique_ptr<LogicalFunction> asField)
{
    this->asField = Util::unique_ptr_dynamic_cast<FieldAccessLogicalFunction>(std::move(asField));
    return std::unique_ptr<WindowAggregationLogicalFunction>(this);
}

LogicalFunction& WindowAggregationLogicalFunction::as() const
{
    if (asField == nullptr)
    {
        return *onField;
    }
    return *asField;
}

std::string WindowAggregationLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "WindowAggregation: ";
    ss << " Type=" << getTypeAsString();
    ss << " onField=" << *onField;
    ss << " asField=" << *asField;
    ss << std::endl;
    return ss.str();
}

WindowAggregationLogicalFunction::Type WindowAggregationLogicalFunction::getType() const
{
    return aggregationType;
}

std::string WindowAggregationLogicalFunction::getTypeAsString() const
{
    return std::string(magic_enum::enum_name(aggregationType));
}

LogicalFunction& WindowAggregationLogicalFunction::on() const
{
    return *onField;
}

DataType& WindowAggregationLogicalFunction::getInputStamp() const
{
    return *inputStamp;
}

DataType& WindowAggregationLogicalFunction::getPartialAggregateStamp() const
{
    return *partialAggregateStamp;
}

DataType& WindowAggregationLogicalFunction::getFinalAggregateStamp() const
{
    return *finalAggregateStamp;
}

bool WindowAggregationLogicalFunction::operator==(std::shared_ptr<WindowAggregationLogicalFunction> otherWindowAggregationLogicalFunction) const
{
    return this->getType() == otherWindowAggregationLogicalFunction->getType() && this->onField == otherWindowAggregationLogicalFunction->onField
        && this->asField == otherWindowAggregationLogicalFunction->asField;
}

}
