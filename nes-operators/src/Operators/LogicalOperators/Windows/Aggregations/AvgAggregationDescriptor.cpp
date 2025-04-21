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
#include <utility>
#include <API/Schema.hpp>
#include <Functions/Expression.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/AvgAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::Windowing
{

AvgAggregationDescriptor::AvgAggregationDescriptor(ExpressionValue field, Schema::Identifier asField)
    : WindowAggregationDescriptor(std::move(field), std::move(asField))
{
    this->aggregationType = Type::Avg;
}

std::shared_ptr<WindowAggregationDescriptor> AvgAggregationDescriptor::create(ExpressionValue onField, Schema::Identifier asField)
{
    return std::make_shared<AvgAggregationDescriptor>(AvgAggregationDescriptor(std::move(onField), std::move(asField)));
}

void AvgAggregationDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField.inferStamp(schema);
    auto stamp = onField.getStamp();
    auto addition = FunctionRegistry::instance().lookup("ADD", {{*stamp, *stamp}});
    /// TODO: Test if addition is associative and commutative

    if (!addition)
    {
        throw NotImplemented("Cannot average {}, because it does not implement addition", *stamp);
    }
    auto division
        = FunctionRegistry::instance().lookup("DIV", {{addition->returnType, DataTypeRegistry::instance().lookup("Integer").value()}});
    if (!division)
    {
        throw NotImplemented("Cannot average {}, because it does not implement division with Integers", *stamp);
    }

    aggregationResultType = division->returnType;
}

std::shared_ptr<WindowAggregationDescriptor> AvgAggregationDescriptor::copy()
{
    return std::make_shared<AvgAggregationDescriptor>(AvgAggregationDescriptor(this->onField, this->asField));
}

}
