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
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <magic_enum/magic_enum.hpp>


namespace NES::Windowing
{

WindowAggregationDescriptor::WindowAggregationDescriptor(ExpressionValue onField, Schema::Identifier asField)
    : onField(std::move(onField)), asField(std::move(asField)), aggregationType(Type::None)
{
}

std::shared_ptr<WindowAggregationDescriptor> WindowAggregationDescriptor::as(Schema::Identifier asField)
{
    this->asField = std::move(asField);
    return this->copy();
}

Schema::Identifier WindowAggregationDescriptor::as() const
{
    return asField;
}

std::string WindowAggregationDescriptor::toString() const
{
    std::stringstream ss;
    ss << "WindowAggregation: ";
    ss << " Type=" << getTypeAsString();
    ss << " onField=" << fmt::format("{}", onField);
    ss << " asField=" << fmt::format("{}", asField);
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

ExpressionValue WindowAggregationDescriptor::on() const
{
    return onField;
}

bool WindowAggregationDescriptor::equal(const std::shared_ptr<WindowAggregationDescriptor>& otherWindowAggregationDescriptor) const
{
    return this->getType() == otherWindowAggregationDescriptor->getType() && this->onField == otherWindowAggregationDescriptor->onField
        && this->asField == otherWindowAggregationDescriptor->asField;
}

}
