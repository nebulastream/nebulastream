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
#include <Operators/LogicalOperators/Windows/Aggregations/SumAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::Windowing
{

SumAggregationDescriptor::SumAggregationDescriptor(ExpressionValue field, Schema::Identifier asField)
    : WindowAggregationDescriptor(std::move(field), std::move(asField))
{
    this->aggregationType = Type::Sum;
}

std::shared_ptr<WindowAggregationDescriptor> SumAggregationDescriptor::create(ExpressionValue onField, Schema::Identifier asField)
{
    return std::make_shared<SumAggregationDescriptor>(SumAggregationDescriptor(std::move(onField), std::move(asField)));
}

void SumAggregationDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField.inferStamp(schema);
    auto stamp = onField.getStamp();

    auto addition = FunctionRegistry::instance().lookup("ADD", {{*stamp, *stamp}});
    if (!addition)
    {
        throw NotImplemented("Cannot Sum {}, because it does not implement addition", *stamp);
    }
    aggregationResultType = addition->returnType;
}

std::shared_ptr<WindowAggregationDescriptor> SumAggregationDescriptor::copy()
{
    return std::make_shared<SumAggregationDescriptor>(SumAggregationDescriptor(onField, asField));
}

}
