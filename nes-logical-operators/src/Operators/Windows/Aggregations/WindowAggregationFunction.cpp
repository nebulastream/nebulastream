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
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Util/Common.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Functions/LogicalFunction.hpp>

namespace NES
{

WindowAggregationFunction::WindowAggregationFunction(const std::shared_ptr<FieldAccessLogicalFunction>& onField)
    : onField(onField), asField(onField)
{
}

WindowAggregationFunction::WindowAggregationFunction(
    const std::shared_ptr<LogicalFunction>& onField, const std::shared_ptr<LogicalFunction>& asField)
    : onField(onField), asField(asField)
{
}

std::shared_ptr<WindowAggregationFunction> WindowAggregationFunction::as(const std::shared_ptr<LogicalFunction>& asField)
{
    const auto& field = NES::Util::as<FieldAccessLogicalFunction>(asField);
    this->asField = field;
    return this->clone();
}

std::shared_ptr<LogicalFunction> WindowAggregationFunction::as() const
{
    if (asField == nullptr)
    {
        return onField;
    }
    return asField;
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

std::shared_ptr<LogicalFunction> WindowAggregationFunction::on() const
{
    return onField;
}

bool WindowAggregationFunction::equal(std::shared_ptr<WindowAggregationFunction> otherWindowAggregationFunction) const
{
    return this->getType() == otherWindowAggregationFunction->getType() && this->onField == otherWindowAggregationFunction->onField
        && this->asField == otherWindowAggregationFunction->asField;
}

}
