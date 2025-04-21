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
#include <Operators/LogicalOperators/Windows/Aggregations/MinAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>


namespace NES::Windowing
{

MinAggregationDescriptor::MinAggregationDescriptor(ExpressionValue field, Schema::Identifier asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Min;
}

std::shared_ptr<WindowAggregationDescriptor> MinAggregationDescriptor::create(ExpressionValue onField, Schema::Identifier asField)
{
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(std::move(onField), std::move(asField)));
}

void MinAggregationDescriptor::inferStamp(const Schema& schema)
{
    /// We first infer the stamp of the input field and set the output stamp as the same.
    onField.inferStamp(schema);
    auto stamp = *onField.getStamp();

    auto greaterEquals = FunctionRegistry::instance().lookup("GreaterEquals", {{stamp, stamp}});
    if (!greaterEquals)
    {
        throw NotImplemented("Cannot do a min aggregations on {}, because it does not implement greater equals", stamp);
    }

    this->aggregationResultType = stamp;
}
std::shared_ptr<WindowAggregationDescriptor> MinAggregationDescriptor::copy()
{
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(this->onField, this->asField));
}

}
