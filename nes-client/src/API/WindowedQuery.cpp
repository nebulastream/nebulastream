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
#include <algorithm>
#include <iterator>
#include <memory>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Functions/Functions.hpp>
#include <API/Query.hpp>
#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES
{

WindowOperatorBuilder::WindowedQuery Query::window(const Windowing::WindowTypePtr& windowType)
{
    return WindowOperatorBuilder::WindowedQuery(*this, windowType);
}

namespace WindowOperatorBuilder
{
WindowedQuery::WindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType)
    : originalQuery(originalQuery), windowType(std::move(windowType))
{
}

KeyedWindowedQuery::KeyedWindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType, std::vector<NodeFunctionPtr> keys)
    : originalQuery(originalQuery), windowType(std::move(windowType)), keys(keys)
{
}

}

Query& Query::window(const Windowing::WindowTypePtr& windowType, std::vector<API::WindowAggregationPtr> aggregations)
{
    std::vector<Windowing::WindowAggregationDescriptorPtr> windowAggregationDescriptors(aggregations.size());
    std::ranges::transform(aggregations, windowAggregationDescriptors.begin(), [](const auto& agg) { return agg->aggregation; });
    this->queryPlan = QueryPlanBuilder::addWindowAggregation(this->queryPlan, windowType, windowAggregationDescriptors, {});
    return *this;
}

Query& Query::windowByKey(
    std::vector<NodeFunctionPtr> keys, const Windowing::WindowTypePtr& windowType, std::vector<API::WindowAggregationPtr> aggregations)
{
    std::vector<Windowing::WindowAggregationDescriptorPtr> windowAggregationDescriptors(aggregations.size());
    std::ranges::transform(aggregations, windowAggregationDescriptors.begin(), [](const auto& agg) { return agg->aggregation; });
    std::vector<std::shared_ptr<NodeFunctionFieldAccess>> onKeysFieldAccess;
    std::ranges::transform(
        keys, std::back_inserter(onKeysFieldAccess), [](const auto& key) { return NES::Util::as<NodeFunctionFieldAccess>(key); });
    this->queryPlan = QueryPlanBuilder::addWindowAggregation(this->queryPlan, windowType, windowAggregationDescriptors, onKeysFieldAccess);
    return *this;
}

}
