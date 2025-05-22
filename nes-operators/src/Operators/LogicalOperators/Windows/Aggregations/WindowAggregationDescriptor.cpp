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
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <magic_enum/magic_enum.hpp>

namespace NES::Windowing
{

WindowAggregationDescriptor::WindowAggregationDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& onField)
    : onField(onField), asField(onField), aggregationType(Type::None)
{
}

WindowAggregationDescriptor::WindowAggregationDescriptor(
    const std::shared_ptr<NodeFunction>& onField, const std::shared_ptr<NodeFunction>& asField)
    : onField(onField), asField(asField), aggregationType(Type::None)
{
}

std::shared_ptr<WindowAggregationDescriptor> WindowAggregationDescriptor::as(const std::shared_ptr<NodeFunction>& asField)
{
    const auto& field = NES::Util::as<NodeFunctionFieldAccess>(asField);
    this->asField = field;
    return this->copy();
}

std::shared_ptr<NodeFunction> WindowAggregationDescriptor::as() const
{
    if (asField == nullptr)
    {
        return onField;
    }
    return asField;
}

std::string WindowAggregationDescriptor::toString() const
{
    std::stringstream ss;
    ss << "WindowAggregation: ";
    ss << " Type=" << getTypeAsString();
    ss << " onField=" << *onField;
    ss << " asField=" << *asField;
    ss << std::endl;
    return ss.str();
}

WindowAggregationDescriptor::Type WindowAggregationDescriptor::getType() const
{
    return aggregationType;
}

std::string WindowAggregationDescriptor::getTypeAsString() const
{
    return std::string(magic_enum::enum_name(aggregationType));
}

std::shared_ptr<NodeFunction> WindowAggregationDescriptor::on() const
{
    return onField;
}

bool WindowAggregationDescriptor::equal(const std::shared_ptr<WindowAggregationDescriptor>& otherWindowAggregationDescriptor) const
{
    return this->getType() == otherWindowAggregationDescriptor->getType() && this->onField->equal(otherWindowAggregationDescriptor->onField)
        && this->asField->equal(otherWindowAggregationDescriptor->asField);
}

}
