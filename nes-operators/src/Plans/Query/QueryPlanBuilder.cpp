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

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <Functions/Expression.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryPlanBuilder.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/WindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Placement/PlacementConstants.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
std::shared_ptr<QueryPlan> QueryPlanBuilder::createQueryPlan(std::string logicalSourceName)
{
    NES_TRACE("QueryPlanBuilder: create query plan for input source  {}", logicalSourceName);
    Configurations::DescriptorConfig::Config SourceDescriptorConfig{};
    auto sourceOperator = std::make_shared<SourceNameLogicalOperator>(logicalSourceName, getNextOperatorId());
    auto queryPlan = QueryPlan::create(sourceOperator);
    queryPlan->setSourceConsumed(logicalSourceName);
    return queryPlan;
}

std::shared_ptr<QueryPlan>
QueryPlanBuilder::addProjection(const std::vector<ExpressionValue>& functions, std::shared_ptr<QueryPlan> queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add projection operator to query plan");
    const std::shared_ptr<Operator> op = std::make_shared<LogicalProjectionOperator>(functions, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addRename(const std::string& newSourceName, std::shared_ptr<QueryPlan> queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add rename operator to query plan");
    auto op = std::make_shared<RenameSourceOperator>(newSourceName, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addSelection(ExpressionValue selectionFunction, std::shared_ptr<QueryPlan> queryPlan)
{
    const std::shared_ptr<Operator> op = std::make_shared<LogicalSelectionOperator>(selectionFunction, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addLimit(const uint64_t limit, std::shared_ptr<QueryPlan> queryPlan)
{
    NES_TRACE("QueryPlanBuilder: add limit operator to query plan");
    const std::shared_ptr<Operator> op = std::make_shared<LogicalLimitOperator>(limit, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addMap(Schema::Identifier asField, ExpressionValue value, std::shared_ptr<QueryPlan> queryPlan)
{
    const std::shared_ptr<Operator> op = std::make_shared<LogicalMapOperator>(getNextOperatorId(), value, asField);
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addWindowAggregation(
    std::shared_ptr<QueryPlan> queryPlan,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    const std::vector<std::shared_ptr<Windowing::WindowAggregationDescriptor>>& windowAggs,
    const std::vector<ExpressionValue>& onKeys)
{
    PRECONDITION(not queryPlan->getRootOperators().empty(), "invalid query plan, as the root operator is empty");

    if (const auto timeBasedWindowType = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType))
    {
        /// check if query contain watermark assigner, and add if missing (as default behaviour)
        if (not NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(queryPlan->getRootOperators()[0]))
            NES_DEBUG("add default watermark strategy as non is provided");

        switch (timeBasedWindowType->getTimeCharacteristic()->getType())
        {
            case Windowing::TimeCharacteristic::Type::IngestionTime:
                queryPlan->appendOperatorAsNewRoot(std::make_shared<WatermarkAssignerLogicalOperator>(
                    Windowing::IngestionTimeWatermarkStrategyDescriptor::create(), getNextOperatorId()));
                break;
            case Windowing::TimeCharacteristic::Type::EventTime:
                queryPlan->appendOperatorAsNewRoot(std::make_shared<WatermarkAssignerLogicalOperator>(
                    Windowing::EventTimeWatermarkStrategyDescriptor::create(
                        timeBasedWindowType->getTimeCharacteristic()->value, timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()),
                    getNextOperatorId()));
                break;
        }
    }
    else
    {
        throw NotImplemented("Only TimeBasedWindowType is supported for now");
    }


    auto inputSchema = queryPlan->getRootOperators()[0]->getOutputSchema();
    const auto windowDefinition = Windowing::LogicalWindowDescriptor::create(onKeys, windowAggs, windowType);
    const auto windowOperator = std::make_shared<LogicalWindowOperator>(windowDefinition, getNextOperatorId());

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return queryPlan;
}

std::shared_ptr<QueryPlan>
QueryPlanBuilder::addUnion(std::shared_ptr<QueryPlan> leftQueryPlan, const std::shared_ptr<QueryPlan>& rightQueryPlan)
{
    NES_TRACE("QueryPlanBuilder: unionWith the subQuery to current query plan");
    const std::shared_ptr<Operator> op = std::make_shared<LogicalUnionOperator>(getNextOperatorId());
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addJoin(
    std::shared_ptr<QueryPlan> leftQueryPlan,
    std::shared_ptr<QueryPlan> rightQueryPlan,
    ExpressionValue joinFunction,
    const std::shared_ptr<Windowing::WindowType>& windowType,
    Join::LogicalJoinDescriptor::JoinType joinType = Join::LogicalJoinDescriptor::JoinType::CARTESIAN_PRODUCT)
{
    NES_TRACE("QueryPlanBuilder: Iterate over all ExpressionNode to check join field.");
    INVARIANT(rightQueryPlan, "invalid rightQueryPlan query plan, it is a nullptr");
    INVARIANT(!rightQueryPlan->getRootOperators().empty(), "RootOperators of rightQueryPlan are empty");
    auto rootOperatorRhs = rightQueryPlan->getRootOperators()[0];
    auto leftJoinType = leftQueryPlan->getRootOperators()[0]->getOutputSchema();
    auto rightQueryPlanJoinType = rootOperatorRhs->getOutputSchema();

    /// check if query contain watermark assigner, and add if missing (as default behaviour)
    leftQueryPlan = checkAndAddWatermarkAssignment(leftQueryPlan, windowType);
    rightQueryPlan = checkAndAddWatermarkAssignment(rightQueryPlan, windowType);

    ///TODO 1,1 should be replaced once we have distributed joins with the number of child input edges
    ///TODO(Ventura?>Steffen) can we know this at this query submission time?
    auto joinDefinition = Join::LogicalJoinDescriptor::create(joinFunction, windowType, 1, 1, joinType);

    NES_TRACE("QueryPlanBuilder: add join operator to query plan");
    auto op = std::make_shared<LogicalJoinOperator>(joinDefinition, getNextOperatorId());
    NES_INFO("Created join {}", *op, Util::as<LogicalJoinOperator>(op)->getJoinDefinition()->getWindowType()->toString());
    leftQueryPlan = addBinaryOperatorAndUpdateSource(op, leftQueryPlan, rightQueryPlan);
    return leftQueryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addSink(std::string sinkName, std::shared_ptr<QueryPlan> queryPlan)
{
    return addSink(std::move(sinkName), std::move(queryPlan), INVALID_WORKER_NODE_ID);
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addSink(std::string sinkName, std::shared_ptr<QueryPlan> queryPlan, WorkerId workerId)
{
    auto sinkOperator = std::make_shared<SinkLogicalOperator>(std::move(sinkName), getNextOperatorId());
    if (workerId != INVALID_WORKER_NODE_ID)
    {
        sinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, workerId);
    }
    const std::shared_ptr<Operator> op = sinkOperator;
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::assignWatermark(
    std::shared_ptr<QueryPlan> queryPlan, const std::shared_ptr<Windowing::WatermarkStrategyDescriptor>& watermarkStrategyDescriptor)
{
    const std::shared_ptr<Operator> op
        = std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor, getNextOperatorId());
    queryPlan->appendOperatorAsNewRoot(op);
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::checkAndAddWatermarkAssignment(
    std::shared_ptr<QueryPlan> queryPlan, const std::shared_ptr<Windowing::WindowType>& windowType)
{
    NES_TRACE("QueryPlanBuilder: checkAndAddWatermarkAssignment for a (sub)query plan");
    auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowType);

    if (queryPlan->getOperatorByType<WatermarkAssignerLogicalOperator>().empty())
    {
        if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
        {
            return assignWatermark(queryPlan, Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
        }
        else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::EventTime)
        {
            return assignWatermark(
                queryPlan,
                Windowing::EventTimeWatermarkStrategyDescriptor::create(
                    timeBasedWindowType->getTimeCharacteristic()->value, timeBasedWindowType->getTimeCharacteristic()->getTimeUnit()));
        }
    }
    return queryPlan;
}

std::shared_ptr<QueryPlan> QueryPlanBuilder::addBinaryOperatorAndUpdateSource(
    const std::shared_ptr<Operator>& operatorNode,
    std::shared_ptr<QueryPlan> leftQueryPlan,
    const std::shared_ptr<QueryPlan>& rightQueryPlan)
{
    leftQueryPlan->addRootOperator(rightQueryPlan->getRootOperators()[0]);
    leftQueryPlan->appendOperatorAsNewRoot(operatorNode);
    NES_TRACE("QueryPlanBuilder: addBinaryOperatorAndUpdateSource: update the source names");
    auto newSourceName = Util::updateSourceName(leftQueryPlan->getSourceConsumed(), rightQueryPlan->getSourceConsumed());
    leftQueryPlan->setSourceConsumed(newSourceName);
    return leftQueryPlan;
}

}
