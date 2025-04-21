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
#include <Operators/LogicalOperators/Windows/Aggregations/CountAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>


namespace NES::Windowing
{

CountAggregationDescriptor::CountAggregationDescriptor(ExpressionValue field, Schema::Identifier asField)
    : WindowAggregationDescriptor(field, asField)
{
    this->aggregationType = Type::Count;
}

std::shared_ptr<WindowAggregationDescriptor> CountAggregationDescriptor::create(ExpressionValue onField, Schema::Identifier asField)
{
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(std::move(onField), std::move(asField)));
}

void CountAggregationDescriptor::inferStamp(const Schema& schema)
{
    onField.inferStamp(schema);
    aggregationResultType = DataTypeRegistry::instance().lookup("Integer").value();
}

std::shared_ptr<WindowAggregationDescriptor> CountAggregationDescriptor::copy()
{
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(this->onField, this->asField));
}

}
