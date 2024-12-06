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

#include <sstream>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <magic_enum.hpp>


namespace NES::Windowing
{

WindowAggregationDescriptor::WindowAggregationDescriptor(const NodeFunctionFieldAccessPtr& onField) : onField(onField), asField(onField)
{
}

WindowAggregationDescriptor::WindowAggregationDescriptor(const NodeFunctionPtr& onField, const NodeFunctionPtr& asField)
    : onField(onField), asField(asField)
{
}

WindowAggregationDescriptorPtr WindowAggregationDescriptor::as(const NodeFunctionPtr& asField)
{
    const auto& field = NES::Util::as<NodeFunctionFieldAccess>(asField);
    this->asField = field;
    return this->copy();
}

NodeFunctionPtr WindowAggregationDescriptor::as() const
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

NodeFunctionPtr WindowAggregationDescriptor::on() const
{
    return onField;
}

bool WindowAggregationDescriptor::equal(WindowAggregationDescriptorPtr otherWindowAggregationDescriptor) const
{
    return this->getType() == otherWindowAggregationDescriptor->getType() && this->onField->equal(otherWindowAggregationDescriptor->onField)
        && this->asField->equal(otherWindowAggregationDescriptor->asField);
}

}
