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

#include <utility>
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalLimitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunction.hpp>
#include <ErrorHandling.hpp>


namespace NES::QueryCompilation
{

DefaultPhysicalOperatorProvider::DefaultPhysicalOperatorProvider(std::shared_ptr<QueryCompilerOptions> options)
    : PhysicalOperatorProvider(std::move(options))
{
}

bool DefaultPhysicalOperatorProvider::isDemultiplex(const LogicalOperatorPtr& operatorNode)
{
    return operatorNode->getParents().size() > 1;
}

void DefaultPhysicalOperatorProvider::insertDemultiplexOperatorsBefore(const LogicalOperatorPtr& operatorNode)
{
    auto operatorOutputSchema = operatorNode->getOutputSchema();
    /// A demultiplex operator has the same output schema as its child operator.
    auto demultiplexOperator = PhysicalOperators::PhysicalDemultiplexOperator::create(operatorOutputSchema);
    demultiplexOperator->setOutputSchema(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndParentNodes(demultiplexOperator);
}

void DefaultPhysicalOperatorProvider::insertMultiplexOperatorsAfter(const LogicalOperatorPtr& operatorNode)
{
    /// the unionOperator operator has the same schema as the output schema of the operator node.
    auto unionOperator = PhysicalOperators::PhysicalUnionOperator::create(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndChildNodes(unionOperator);
}

void DefaultPhysicalOperatorProvider::lower(DecomposedQueryPlanPtr decomposedQueryPlan, LogicalOperatorPtr operatorNode)
{
    if (isDemultiplex(operatorNode))
    {
        insertDemultiplexOperatorsBefore(operatorNode);
    }

    if (NES::Util::instanceOf<UnaryOperator>(operatorNode))
    {
        lowerUnaryOperator(decomposedQueryPlan, operatorNode);
    }
    else if (NES::Util::instanceOf<BinaryOperator>(operatorNode))
    {
        lowerBinaryOperator(operatorNode);
    }
    else
    {
        NES_NOT_IMPLEMENTED();
    }

    NES_DEBUG("DefaultPhysicalOperatorProvider:: Plan after lowering \n{}", decomposedQueryPlan->toString());
}

void DefaultPhysicalOperatorProvider::lowerUnaryOperator(
    const DecomposedQueryPlanPtr& decomposedQueryPlan, const LogicalOperatorPtr& operatorNode)
{
    PRECONDITION(operatorNode->getParents().size() <= 1, "A unary operator should have at most one parent.");

    /// If a unary operator has more than one parent, we introduce an implicit multiplex operator before.
    if (operatorNode->getChildren().size() > 1)
    {
        insertMultiplexOperatorsAfter(operatorNode);
    }

    if (NES::Util::instanceOf<SourceLogicalOperator>(operatorNode))
    {
        auto logicalSourceOperator = NES::Util::as<SourceLogicalOperator>(operatorNode);
        auto physicalSourceOperator = PhysicalOperators::PhysicalSourceOperator::create(
            getNextOperatorId(),
            logicalSourceOperator->getOriginId(),
            logicalSourceOperator->getInputSchema(),
            logicalSourceOperator->getOutputSchema(),
            std::make_shared<Sources::SourceDescriptor>(logicalSourceOperator->getSourceDescriptorRef()));
        physicalSourceOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(physicalSourceOperator);
    }
    else if (NES::Util::instanceOf<SinkLogicalOperator>(operatorNode))
    {
        auto logicalSinkOperator = NES::Util::as<SinkLogicalOperator>(operatorNode);

        auto physicalSinkOperator = PhysicalOperators::PhysicalSinkOperator::create(
            logicalSinkOperator->getInputSchema(), logicalSinkOperator->getOutputSchema(), logicalSinkOperator->getSinkDescriptor());
        physicalSinkOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(physicalSinkOperator);
        decomposedQueryPlan->replaceRootOperator(logicalSinkOperator, physicalSinkOperator);
    }
    else if (NES::Util::instanceOf<LogicalFilterOperator>(operatorNode))
    {
        auto filterOperator = NES::Util::as<LogicalFilterOperator>(operatorNode);
        auto physicalFilterOperator = PhysicalOperators::PhysicalFilterOperator::create(
            filterOperator->getInputSchema(), filterOperator->getOutputSchema(), filterOperator->getPredicate());
        physicalFilterOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(physicalFilterOperator);
    }
    else if (NES::Util::instanceOf<WindowOperator>(operatorNode))
    {
        lowerWindowOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<WatermarkAssignerLogicalOperator>(operatorNode))
    {
        lowerWatermarkAssignmentOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<LogicalMapOperator>(operatorNode))
    {
        lowerMapOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<InferModel::LogicalInferModelOperator>(operatorNode))
    {
        lowerInferModelOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<LogicalProjectionOperator>(operatorNode))
    {
        lowerProjectOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<LogicalLimitOperator>(operatorNode))
    {
        auto limitOperator = NES::Util::as<LogicalLimitOperator>(operatorNode);
        auto physicalLimitOperator = PhysicalOperators::PhysicalLimitOperator::create(
            limitOperator->getInputSchema(), limitOperator->getOutputSchema(), limitOperator->getLimit());
        operatorNode->replace(physicalLimitOperator);
    }
    else
    {
        throw UnknownLogicalOperator();
    }
}

void DefaultPhysicalOperatorProvider::lowerBinaryOperator(const LogicalOperatorPtr& operatorNode)
{
    PRECONDITION(
        NES::Util::instanceOf<LogicalUnionOperator>(operatorNode) || NES::Util::instanceOf<LogicalJoinOperator>(operatorNode),
        operatorNode->toString() + " is no binaryOperator")
    if (NES::Util::instanceOf<LogicalUnionOperator>(operatorNode))
    {
        lowerUnionOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<LogicalJoinOperator>(operatorNode))
    {
        lowerJoinOperator(operatorNode);
    }
}

void DefaultPhysicalOperatorProvider::lowerUnionOperator(const LogicalOperatorPtr& operatorNode)
{
    auto unionOperator = NES::Util::as<LogicalUnionOperator>(operatorNode);
    /// this assumes that we apply the ProjectBeforeUnionRule and the input across all children is the same.
    PRECONDITION(
        unionOperator->getLeftInputSchema()->equals(unionOperator->getRightInputSchema()),
        "The input schemas of a union operators children should be equal");

    auto physicalUnionOperator = PhysicalOperators::PhysicalUnionOperator::create(unionOperator->getLeftInputSchema());
    operatorNode->replace(physicalUnionOperator);
}

void DefaultPhysicalOperatorProvider::lowerProjectOperator(const LogicalOperatorPtr& operatorNode)
{
    auto projectOperator = NES::Util::as<LogicalProjectionOperator>(operatorNode);
    auto physicalProjectOperator = PhysicalOperators::PhysicalProjectOperator::create(
        projectOperator->getInputSchema(), projectOperator->getOutputSchema(), projectOperator->getExpressions());

    physicalProjectOperator->addProperty("LogicalOperatorId", projectOperator->getId());
    operatorNode->replace(physicalProjectOperator);
}

void DefaultPhysicalOperatorProvider::lowerInferModelOperator(LogicalOperatorPtr operatorNode)
{
    auto inferModelOperator = NES::Util::as<InferModel::LogicalInferModelOperator>(operatorNode);
    auto physicalInferModelOperator = PhysicalOperators::PhysicalInferModelOperator::create(
        inferModelOperator->getInputSchema(),
        inferModelOperator->getOutputSchema(),
        inferModelOperator->getModel(),
        inferModelOperator->getInputFields(),
        inferModelOperator->getOutputFields());
    operatorNode->replace(physicalInferModelOperator);
}

void DefaultPhysicalOperatorProvider::lowerMapOperator(const LogicalOperatorPtr& operatorNode)
{
    auto mapOperator = NES::Util::as<LogicalMapOperator>(operatorNode);
    auto physicalMapOperator = PhysicalOperators::PhysicalMapOperator::create(
        mapOperator->getInputSchema(), mapOperator->getOutputSchema(), mapOperator->getMapExpression());
    physicalMapOperator->addProperty("LogicalOperatorId", operatorNode->getId());
    operatorNode->replace(physicalMapOperator);
}

OperatorPtr DefaultPhysicalOperatorProvider::getJoinBuildInputOperator(
    const LogicalJoinOperatorPtr& joinOperator, SchemaPtr outputSchema, std::vector<OperatorPtr> children)
{
    PRECONDITION(!children.empty(), "There should be at least one child for the join operator " + joinOperator->toString());

    if (children.size() > 1)
    {
        auto demultiplexOperator = PhysicalOperators::PhysicalUnionOperator::create(std::move(outputSchema));
        demultiplexOperator->setOutputSchema(joinOperator->getOutputSchema());
        demultiplexOperator->addParent(joinOperator);
        for (const auto& child : children)
        {
            child->removeParent(joinOperator);
            child->addParent(demultiplexOperator);
        }
        return demultiplexOperator;
    }
    return children[0];
}

void DefaultPhysicalOperatorProvider::lowerJoinOperator(const LogicalOperatorPtr& operatorNode)
{
    lowerNautilusJoin(operatorNode);
}

void DefaultPhysicalOperatorProvider::lowerNautilusJoin(const LogicalOperatorPtr& operatorNode)
{
    using namespace Runtime::Execution::Operators;

    auto joinOperator = NES::Util::as<LogicalJoinOperator>(operatorNode);
    const auto& joinDefinition = joinOperator->getJoinDefinition();

    /// returns the following pair:  std::make_pair(leftJoinKeyNameEqui,rightJoinKeyNameEqui);
    auto equiJoinKeyNames = NES::findEquiJoinKeyNames(joinDefinition->getJoinExpression());

    const auto windowType = Util::as<Windowing::TimeBasedWindowType>(joinDefinition->getWindowType());
    const auto& windowSize = windowType->getSize().getTime();
    const auto& windowSlide = windowType->getSlide().getTime();
    if (!(Util::instanceOf<Windowing::TumblingWindow>(windowType) || Util::instanceOf<Windowing::SlidingWindow>(windowType)))
    {
        throw UnknownWindowType();
    }

    const auto [timeStampFieldLeft, timeStampFieldRight] = getTimestampLeftAndRight(joinOperator, windowType);
    const auto leftInputOperator
        = getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
    const auto rightInputOperator
        = getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());
    const auto joinStrategy = options->joinStrategy;

    const StreamJoinOperators streamJoinOperators(operatorNode, leftInputOperator, rightInputOperator);
    const StreamJoinConfigs streamJoinConfig(
        equiJoinKeyNames.first, equiJoinKeyNames.second, windowSize, windowSlide, timeStampFieldLeft, timeStampFieldRight, joinStrategy);

    StreamJoinOperatorHandlerPtr joinOperatorHandler;
    switch (joinStrategy)
    {
        case StreamJoinStrategy::HASH_JOIN_LOCAL:
        case StreamJoinStrategy::HASH_JOIN_VAR_SIZED:
        case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING:
        case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE:
            joinOperatorHandler = lowerStreamingHashJoin(streamJoinOperators, streamJoinConfig);
            break;
        case StreamJoinStrategy::NESTED_LOOP_JOIN:
            joinOperatorHandler = lowerStreamingNestedLoopJoin(streamJoinOperators, streamJoinConfig);
            break;
    }

    auto createBuildOperator = [&](const SchemaPtr& inputSchema,
                                   JoinBuildSideType buildSideType,
                                   const TimestampField& timeStampField,
                                   const std::string& joinFieldName)
    {
        return PhysicalOperators::PhysicalStreamJoinBuildOperator::create(
            inputSchema,
            joinOperator->getOutputSchema(),
            joinOperatorHandler,
            buildSideType,
            timeStampField,
            joinFieldName,
            options->joinStrategy);
    };

    const auto leftJoinBuildOperator = createBuildOperator(
        joinOperator->getLeftInputSchema(),
        JoinBuildSideType::Left,
        streamJoinConfig.timeStampFieldLeft,
        streamJoinConfig.joinFieldNameLeft);
    const auto rightJoinBuildOperator = createBuildOperator(
        joinOperator->getRightInputSchema(),
        JoinBuildSideType::Right,
        streamJoinConfig.timeStampFieldRight,
        streamJoinConfig.joinFieldNameRight);
    const auto joinProbeOperator = PhysicalOperators::PhysicalStreamJoinProbeOperator::create(
        joinOperator->getLeftInputSchema(),
        joinOperator->getRightInputSchema(),
        joinOperator->getOutputSchema(),
        joinOperator->getJoinExpression(),
        joinOperator->getWindowStartFieldName(),
        joinOperator->getWindowEndFieldName(),
        joinOperatorHandler,
        options->joinStrategy);

    streamJoinOperators.leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
    streamJoinOperators.rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
    streamJoinOperators.operatorNode->replace(joinProbeOperator);
}

Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr DefaultPhysicalOperatorProvider::lowerStreamingNestedLoopJoin(
    const StreamJoinOperators& streamJoinOperators, const StreamJoinConfigs& streamJoinConfig)
{
    using namespace Runtime::Execution;
    const auto joinOperator = NES::Util::as<LogicalJoinOperator>(streamJoinOperators.operatorNode);

    Operators::NLJOperatorHandlerPtr joinOperatorHandler;
    return Operators::NLJOperatorHandlerSlicing::create(
        joinOperator->getAllInputOriginIds(),
        joinOperator->getOutputOriginIds()[0],
        streamJoinConfig.windowSize,
        streamJoinConfig.windowSlide,
        joinOperator->getLeftInputSchema(),
        joinOperator->getRightInputSchema(),
        Nautilus::Interface::PagedVectorVarSized::PAGE_SIZE,
        Nautilus::Interface::PagedVectorVarSized::PAGE_SIZE);
}

Runtime::Execution::Operators::StreamJoinOperatorHandlerPtr DefaultPhysicalOperatorProvider::lowerStreamingHashJoin(
    const StreamJoinOperators& streamJoinOperators, const StreamJoinConfigs& streamJoinConfig)
{
    using namespace Runtime::Execution;
    const auto joinOperator = NES::Util::as<LogicalJoinOperator>(streamJoinOperators.operatorNode);
    Operators::HJOperatorHandlerPtr joinOperatorHandler;
    return Operators::HJOperatorHandlerSlicing::create(
        joinOperator->getAllInputOriginIds(),
        joinOperator->getOutputOriginIds()[0],
        streamJoinConfig.windowSize,
        streamJoinConfig.windowSlide,
        joinOperator->getLeftInputSchema(),
        joinOperator->getRightInputSchema(),
        streamJoinConfig.joinStrategy,
        options->hashJoinOptions.totalSizeForDataStructures,
        options->hashJoinOptions.preAllocPageCnt,
        options->hashJoinOptions.pageSize,
        options->hashJoinOptions.numberOfPartitions);
}

std::tuple<TimestampField, TimestampField> DefaultPhysicalOperatorProvider::getTimestampLeftAndRight(
    const std::shared_ptr<LogicalJoinOperator>& joinOperator, const Windowing::TimeBasedWindowTypePtr& windowType) const
{
    if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
    {
        NES_DEBUG("Skip eventime identification as we use ingestion time");
        return {TimestampField::IngestionTime(), TimestampField::IngestionTime()};
    }
    else
    {
        /// FIXME Once #3407 is done, we can change this to get the left and right fieldname
        auto timeStampFieldName = windowType->getTimeCharacteristic()->getField()->getName();
        auto timeStampFieldNameWithoutSourceName = timeStampFieldName.substr(timeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR));

        /// Lambda function for extracting the timestamp from a schema
        auto findTimeStampFieldName = [&](const SchemaPtr& schema)
        {
            for (const auto& field : schema->fields)
            {
                if (field->getName().find(timeStampFieldNameWithoutSourceName) != std::string::npos)
                {
                    return field->getName();
                }
            }
            return std::string();
        };

        /// Extracting the left and right timestamp
        auto timeStampFieldNameLeft = findTimeStampFieldName(joinOperator->getLeftInputSchema());
        auto timeStampFieldNameRight = findTimeStampFieldName(joinOperator->getRightInputSchema());

        NES_ASSERT(
            !(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
            "Could not find timestampfieldname " << timeStampFieldNameWithoutSourceName << " in both streams!");
        NES_DEBUG("timeStampFieldNameLeft:{}  timeStampFieldNameRight:{} ", timeStampFieldNameLeft, timeStampFieldNameRight);

        return {
            TimestampField::EventTime(timeStampFieldNameLeft, windowType->getTimeCharacteristic()->getTimeUnit()),
            TimestampField::EventTime(timeStampFieldNameRight, windowType->getTimeCharacteristic()->getTimeUnit())};
    }
}

void DefaultPhysicalOperatorProvider::lowerWatermarkAssignmentOperator(const LogicalOperatorPtr& operatorNode)
{
    auto logicalWatermarkAssignment = NES::Util::as<WatermarkAssignerLogicalOperator>(operatorNode);
    auto physicalWatermarkAssignment = PhysicalOperators::PhysicalWatermarkAssignmentOperator::create(
        logicalWatermarkAssignment->getInputSchema(),
        logicalWatermarkAssignment->getOutputSchema(),
        logicalWatermarkAssignment->getWatermarkStrategyDescriptor());
    physicalWatermarkAssignment->setOutputSchema(logicalWatermarkAssignment->getOutputSchema());
    operatorNode->replace(physicalWatermarkAssignment);
}

void DefaultPhysicalOperatorProvider::lowerTimeBasedWindowOperator(const LogicalOperatorPtr& operatorNode)
{
    NES_DEBUG("Create Thread local window aggregation");
    auto windowOperator = NES::Util::as<WindowOperator>(operatorNode);
    PRECONDITION(!windowOperator->getInputOriginIds().empty(), "The number of input origin IDs for an window operator should not be zero.");

    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();

    auto timeBasedWindowType = Util::as<Windowing::TimeBasedWindowType>(windowDefinition->getWindowType());
    auto windowOperatorProperties = WindowOperatorProperties(windowOperator, windowInputSchema, windowOutputSchema, windowDefinition);

    /// TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());
    windowDefinition->setInputOriginIds(windowOperator->getInputOriginIds());

    auto preAggregationOperator = PhysicalOperators::PhysicalSlicePreAggregationOperator::create(
        getNextOperatorId(), windowInputSchema, windowOutputSchema, windowDefinition);
    operatorNode->insertBetweenThisAndChildNodes(preAggregationOperator);

    /// if we have a sliding window and use slicing we have to create another slice merge operator
    if (Util::instanceOf<Windowing::SlidingWindow>(timeBasedWindowType))
    {
        auto mergingOperator = PhysicalOperators::PhysicalSliceMergingOperator::create(
            getNextOperatorId(), windowInputSchema, windowOutputSchema, windowDefinition);
        operatorNode->insertBetweenThisAndChildNodes(mergingOperator);
    }
    auto windowSink = PhysicalOperators::PhysicalWindowSinkOperator::create(
        getNextOperatorId(), windowInputSchema, windowOutputSchema, windowDefinition);
    operatorNode->replace(windowSink);
}

void DefaultPhysicalOperatorProvider::lowerWindowOperator(const LogicalOperatorPtr& operatorNode)
{
    auto windowOperator = NES::Util::as<WindowOperator>(operatorNode);
    PRECONDITION(windowOperator->getInputOriginIds().empty(), "The number of input origin IDs for an window operator should not be zero.");
    PRECONDITION(NES::Util::instanceOf<LogicalWindowOperator>(operatorNode), "The operator should be a window operator.");

    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();
    /// TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());

    /// handle if threshold window
    ///TODO: At this point we are already a central window, we do not want the threshold window to become a Gentral Window in the first place
    auto windowType = NES::Util::as<WindowOperator>(operatorNode)->getWindowDefinition()->getWindowType();
    if (Util::instanceOf<Windowing::ContentBasedWindowType>(windowType))
    {
        auto contentBasedWindowType = Util::as<Windowing::ContentBasedWindowType>(windowType);
        /// check different content-based window types
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            NES_INFO("Lower ThresholdWindow");
            auto thresholdWindowPhysicalOperator
                = PhysicalOperators::PhysicalThresholdWindowOperator::create(windowInputSchema, windowOutputSchema, windowDefinition);
            thresholdWindowPhysicalOperator->addProperty("LogicalOperatorId", operatorNode->getId());

            operatorNode->replace(thresholdWindowPhysicalOperator);
            return;
        }
        else
        {
            throw UnknownWindowType(windowDefinition->getWindowType()->toString());
        }
    }
    else if (Util::instanceOf<Windowing::TimeBasedWindowType>(windowType))
    {
        lowerTimeBasedWindowOperator(operatorNode);
    }
}

}
