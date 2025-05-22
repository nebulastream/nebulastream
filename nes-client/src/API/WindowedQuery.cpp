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
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Functions/Functions.hpp>
#include <API/Query.hpp>
#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/WindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

WindowOperatorBuilder::WindowedQuery Query::window(const std::shared_ptr<Windowing::WindowType>& windowType)
{
    return WindowOperatorBuilder::WindowedQuery(*this, windowType);
}

namespace WindowOperatorBuilder
{
WindowedQuery::WindowedQuery(Query& originalQuery, std::shared_ptr<Windowing::WindowType> windowType)
    : originalQuery(originalQuery), windowType(std::move(windowType))
{
}

KeyedWindowedQuery::KeyedWindowedQuery(
    Query& originalQuery, std::shared_ptr<Windowing::WindowType> windowType, std::vector<std::shared_ptr<NodeFunction>> keys)
    : originalQuery(originalQuery), windowType(std::move(windowType)), keys(std::move(keys))
{
}

}

Query&
Query::window(const std::shared_ptr<Windowing::WindowType>& windowType, std::vector<std::shared_ptr<API::WindowAggregation>> aggregations)
{
    std::vector<std::shared_ptr<Windowing::WindowAggregationDescriptor>> windowAggregationDescriptors(aggregations.size());
    std::ranges::transform(aggregations, windowAggregationDescriptors.begin(), [](const auto& agg) { return agg->aggregation; });
    this->queryPlan = QueryPlanBuilder::addWindowAggregation(this->queryPlan, windowType, windowAggregationDescriptors, {});
    return *this;
}

Query& Query::windowByKey(
    std::vector<std::shared_ptr<NodeFunction>> keys,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    std::vector<std::shared_ptr<API::WindowAggregation>> aggregations)
{
    std::vector<std::shared_ptr<Windowing::WindowAggregationDescriptor>> windowAggregationDescriptors(aggregations.size());
    std::ranges::transform(aggregations, windowAggregationDescriptors.begin(), [](const auto& agg) { return agg->aggregation; });
    std::vector<std::shared_ptr<NodeFunctionFieldAccess>> onKeysFieldAccess;
    std::ranges::transform(
        keys, std::back_inserter(onKeysFieldAccess), [](const auto& key) { return NES::Util::as<NodeFunctionFieldAccess>(key); });
    this->queryPlan = QueryPlanBuilder::addWindowAggregation(this->queryPlan, windowType, windowAggregationDescriptors, onKeysFieldAccess);
    return *this;
}

}
