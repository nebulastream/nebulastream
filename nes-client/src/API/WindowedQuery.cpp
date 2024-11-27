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
#include <API/AttributeField.hpp>
#include <API/Functions/Functions.hpp>
#include <API/Query.hpp>
#include <API/WindowedQuery.hpp>
#include <API/Windowing.hpp>
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
    NES_DEBUG("Query: add window operator");

    uint64_t allowedLateness = 0;
    if (Util::instanceOf<Windowing::TimeBasedWindowType>(windowType))
    {
        auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);
        if (!NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(queryPlan->getRootOperators()[0]))
        {
            NES_DEBUG("add default watermark strategy as non is provided");
            if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
            {
                queryPlan->appendOperatorAsNewRoot(std::make_shared<WatermarkAssignerLogicalOperator>(
                    Windowing::IngestionTimeWatermarkStrategyDescriptor::create(), getNextOperatorId()));
            }
            else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::EventTime)
            {
                queryPlan->appendOperatorAsNewRoot(std::make_shared<WatermarkAssignerLogicalOperator>(
                    Windowing::EventTimeWatermarkStrategyDescriptor::create(
                        NodeFunctionFieldAccess::create(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                        API::Milliseconds(0),
                        timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()),
                    getNextOperatorId()));
            }
        }
        else
        {
            NES_DEBUG("add existing watermark strategy for window");
            auto assigner = NES::Util::as<WatermarkAssignerLogicalOperator>(queryPlan->getRootOperators()[0]);
            if (auto eventTimeWatermarkStrategyDescriptor
                = std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(assigner->getWatermarkStrategyDescriptor()))
            {
                allowedLateness = eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime();
            }
            else if (
                auto ingestionTimeWatermarkDescriptior = std::dynamic_pointer_cast<Windowing::IngestionTimeWatermarkStrategyDescriptor>(
                    assigner->getWatermarkStrategyDescriptor()))
            {
                NES_WARNING("Note: ingestion time does not support allowed lateness yet");
            }
            else
            {
                NES_ERROR("cannot create watermark strategy from descriptor");
            }
        }
    }

    auto inputSchema = getQueryPlan()->getRootOperators()[0]->getOutputSchema();
    std::vector<Windowing::WindowAggregationDescriptorPtr> windowAggregationDescriptors;
    windowAggregationDescriptors.reserve(aggregations.size());
    for (auto const& agg : aggregations)
    {
        windowAggregationDescriptors.emplace_back(agg->aggregation);
    }
    auto windowDefinition = Windowing::LogicalWindowDescriptor::create(windowAggregationDescriptors, windowType, allowedLateness);
    auto windowOperator = std::make_shared<LogicalWindowOperator>(windowDefinition, getNextOperatorId());

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

Query& Query::windowByKey(
    std::vector<NodeFunctionPtr> onKeys, const Windowing::WindowTypePtr& windowType, std::vector<API::WindowAggregationPtr> aggregations)
{
    NES_DEBUG("Query: add keyed window operator");
    std::vector<NodeFunctionFieldAccessPtr> nodeFunctions;
    for (const auto& onKey : onKeys)
    {
        if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(onKey))
        {
            NES_ERROR("Query: window key has to be an FieldAccessFunction but it was a {}", *onKey);
        }
        nodeFunctions.emplace_back(NES::Util::as<NodeFunctionFieldAccess>(onKey));
    }

    uint64_t allowedLateness = 0;
    if (Util::instanceOf<Windowing::TimeBasedWindowType>(windowType))
    {
        auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);
        /// check if query contain watermark assigner, and add if missing (as default behaviour)
        if (!NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(queryPlan->getRootOperators()[0]))
        {
            NES_DEBUG("add default watermark strategy as non is provided");
            if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
            {
                queryPlan->appendOperatorAsNewRoot(std::make_shared<WatermarkAssignerLogicalOperator>(
                    Windowing::IngestionTimeWatermarkStrategyDescriptor::create(), getNextOperatorId()));
            }
            else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::EventTime)
            {
                queryPlan->appendOperatorAsNewRoot(std::make_shared<WatermarkAssignerLogicalOperator>(
                    Windowing::EventTimeWatermarkStrategyDescriptor::create(
                        NodeFunctionFieldAccess::create(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                        API::Milliseconds(0),
                        timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()),
                    getNextOperatorId()));
            }
        }
        else
        {
            NES_DEBUG("add existing watermark strategy for window");
            auto assigner = NES::Util::as<WatermarkAssignerLogicalOperator>(queryPlan->getRootOperators()[0]);
            if (auto eventTimeWatermarkStrategyDescriptor
                = std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(assigner->getWatermarkStrategyDescriptor()))
            {
                allowedLateness = eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime();
            }
            else if (
                auto ingestionTimeWatermarkDescriptior = std::dynamic_pointer_cast<Windowing::IngestionTimeWatermarkStrategyDescriptor>(
                    assigner->getWatermarkStrategyDescriptor()))
            {
                NES_WARNING("Note: ingestion time does not support allowed lateness yet");
            }
            else
            {
                NES_ERROR("cannot create watermark strategy from descriptor");
            }
        }
    }

    auto inputSchema = getQueryPlan()->getRootOperators()[0]->getOutputSchema();

    std::vector<Windowing::WindowAggregationDescriptorPtr> windowAggregationDescriptors;
    windowAggregationDescriptors.reserve(aggregations.size());
    for (auto const& agg : aggregations)
    {
        windowAggregationDescriptors.emplace_back(agg->aggregation);
    }

    auto windowDefinition
        = Windowing::LogicalWindowDescriptor::create(nodeFunctions, windowAggregationDescriptors, windowType, allowedLateness);
    auto windowOperator = std::make_shared<LogicalWindowOperator>(windowDefinition, getNextOperatorId());

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

}
