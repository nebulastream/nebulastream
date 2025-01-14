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
#include <memory>
#include <API/Schema.hpp>
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Execution/Operators/SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalSelectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalLimitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationBuild.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationProbe.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/Common.hpp>
#include <Util/Execution.hpp>
#include <Util/Logger/Logger.hpp>
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
        lowerUnaryOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<BinaryOperator>(operatorNode))
    {
        lowerBinaryOperator(operatorNode);
    }
    else
    {
        throw FunctionNotImplemented("DefaultPhysicalOperatorProvider: lowering of this Operator not supported");
    }

    NES_DEBUG("DefaultPhysicalOperatorProvider:: Plan after lowering \n{}", decomposedQueryPlan->toString());
}

void DefaultPhysicalOperatorProvider::lowerUnaryOperator(const LogicalOperatorPtr& operatorNode)
{
    PRECONDITION(operatorNode->getParents().size() <= 1, "A unary operator should have at most one parent.");

    /// If a unary operator has more than one parent, we introduce an implicit multiplex operator before.
    if (operatorNode->getChildren().size() > 1)
    {
        insertMultiplexOperatorsAfter(operatorNode);
    }

    if (NES::Util::instanceOf<LogicalSelectionOperator>(operatorNode))
    {
        auto filterOperator = NES::Util::as<LogicalSelectionOperator>(operatorNode);
        auto PhysicalSelectionOperator = PhysicalOperators::PhysicalSelectionOperator::create(
            filterOperator->getInputSchema(), filterOperator->getOutputSchema(), filterOperator->getPredicate());
        PhysicalSelectionOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(PhysicalSelectionOperator);
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
        throw UnknownLogicalOperator("Operator: {}", *operatorNode);
    }
}

void DefaultPhysicalOperatorProvider::lowerBinaryOperator(const LogicalOperatorPtr& operatorNode)
{
    PRECONDITION(
        NES::Util::instanceOf<LogicalUnionOperator>(operatorNode) || NES::Util::instanceOf<LogicalJoinOperator>(operatorNode),
        "{} is no binaryOperator",
        *operatorNode)
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
        *unionOperator->getLeftInputSchema() == *unionOperator->getRightInputSchema(),
        "The input schemas of a union operators children should be equal");

    auto physicalUnionOperator = PhysicalOperators::PhysicalUnionOperator::create(unionOperator->getLeftInputSchema());
    operatorNode->replace(physicalUnionOperator);
}

void DefaultPhysicalOperatorProvider::lowerProjectOperator(const LogicalOperatorPtr& operatorNode)
{
    auto projectOperator = NES::Util::as<LogicalProjectionOperator>(operatorNode);
    auto physicalProjectOperator = PhysicalOperators::PhysicalProjectOperator::create(
        projectOperator->getInputSchema(), projectOperator->getOutputSchema(), projectOperator->getFunctions());

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
        mapOperator->getInputSchema(), mapOperator->getOutputSchema(), mapOperator->getMapFunction());
    physicalMapOperator->addProperty("LogicalOperatorId", operatorNode->getId());
    operatorNode->replace(physicalMapOperator);
}

std::shared_ptr<Operator> DefaultPhysicalOperatorProvider::getJoinBuildInputOperator(
    const LogicalJoinOperatorPtr& joinOperator, SchemaPtr outputSchema, std::vector<std::shared_ptr<Operator>> children)
{
    PRECONDITION(!children.empty(), "There should be at least one child for the join operator {}", *joinOperator);

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
    using namespace Runtime::Execution::Operators;

    auto joinOperator = NES::Util::as<LogicalJoinOperator>(operatorNode);
    const auto& joinDefinition = joinOperator->getJoinDefinition();

    auto getJoinFieldNames = [](const SchemaPtr& inputSchema, const NodeFunctionPtr& joinFunction)
    {
        std::vector<std::string> joinFieldNames;
        std::vector<std::string> fieldNamesInJoinFunction;
        std::ranges::for_each(
            joinFunction->getAndFlattenAllChildren(false),
            [&fieldNamesInJoinFunction](const auto& child)
            {
                if (NES::Util::instanceOf<NodeFunctionFieldAccess>(child))
                {
                    fieldNamesInJoinFunction.push_back(NES::Util::as<NodeFunctionFieldAccess>(child)->getFieldName());
                }
            });

        for (const auto& field : *inputSchema)
        {
            if (std::ranges::find(fieldNamesInJoinFunction, field->getName()) != fieldNamesInJoinFunction.end())
            {
                joinFieldNames.push_back(field->getName());
            }
        }

        return joinFieldNames;
    };


    const auto& joinFieldNameLeft = getJoinFieldNames(joinDefinition->getLeftSourceType(), joinDefinition->getJoinFunction());
    const auto& joinFieldNameRight = getJoinFieldNames(joinDefinition->getRightSourceType(), joinDefinition->getJoinFunction());

    const auto windowType = NES::Util::as<Windowing::TimeBasedWindowType>(joinDefinition->getWindowType());
    const auto& windowSize = windowType->getSize().getTime();
    const auto& windowSlide = windowType->getSlide().getTime();
    NES_ASSERT(
        NES::Util::instanceOf<Windowing::TumblingWindow>(windowType) || NES::Util::instanceOf<Windowing::SlidingWindow>(windowType),
        "Only a tumbling or sliding window is currently supported for StreamJoin");

    const auto [timeStampFieldLeft, timeStampFieldRight] = getTimestampLeftAndRight(joinOperator, windowType);
    const auto leftInputOperator
        = getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
    const auto rightInputOperator
        = getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());
    const auto joinStrategy = options->joinStrategy;

    const StreamJoinOperators streamJoinOperators(operatorNode, leftInputOperator, rightInputOperator);
    const StreamJoinConfigs streamJoinConfig(
        joinFieldNameLeft, joinFieldNameRight, windowSize, windowSlide, timeStampFieldLeft, timeStampFieldRight, joinStrategy);

    std::shared_ptr<StreamJoinOperatorHandler> joinOperatorHandler;
    switch (joinStrategy)
    {
        case StreamJoinStrategy::NESTED_LOOP_JOIN:
            joinOperatorHandler = lowerStreamingNestedLoopJoin(streamJoinOperators, streamJoinConfig);
            break;
    }

    auto createBuildOperator = [&](const SchemaPtr& inputSchema, JoinBuildSideType buildSideType, const TimestampField& timeStampField)
    {
        return std::make_shared<PhysicalOperators::PhysicalStreamJoinBuildOperator>(
            inputSchema, joinOperator->getOutputSchema(), joinOperatorHandler, options->joinStrategy, timeStampField, buildSideType);
    };
    auto joinFunctionLowered = FunctionProvider::lowerFunction(joinOperator->getJoinFunction());
    const auto leftJoinBuildOperator
        = createBuildOperator(joinOperator->getLeftInputSchema(), JoinBuildSideType::Left, streamJoinConfig.timeStampFieldLeft);
    const auto rightJoinBuildOperator
        = createBuildOperator(joinOperator->getRightInputSchema(), JoinBuildSideType::Right, streamJoinConfig.timeStampFieldRight);
    const auto joinProbeOperator = std::make_shared<PhysicalOperators::PhysicalStreamJoinProbeOperator>(
        joinOperator->getLeftInputSchema(),
        joinOperator->getRightInputSchema(),
        joinOperator->getOutputSchema(),
        joinOperatorHandler,
        options->joinStrategy,
        std::move(joinFunctionLowered),
        streamJoinConfig.joinFieldNamesLeft,
        streamJoinConfig.joinFieldNamesRight,
        joinOperator->getWindowStartFieldName(),
        joinOperator->getWindowEndFieldName());

    streamJoinOperators.leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
    streamJoinOperators.rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
    streamJoinOperators.operatorNode->replace(joinProbeOperator);
}

std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler> DefaultPhysicalOperatorProvider::lowerStreamingNestedLoopJoin(
    const StreamJoinOperators& streamJoinOperators, const StreamJoinConfigs& streamJoinConfig)
{
    using namespace Runtime::Execution;
    const auto joinOperator = NES::Util::as<LogicalJoinOperator>(streamJoinOperators.operatorNode);

    auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        options->windowOperatorOptions.pageSize, joinOperator->getLeftInputSchema());
    leftMemoryProvider->getMemoryLayoutPtr()->setKeyFieldNames(streamJoinConfig.joinFieldNamesLeft);
    auto rightMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        options->windowOperatorOptions.pageSize, joinOperator->getRightInputSchema());
    rightMemoryProvider->getMemoryLayoutPtr()->setKeyFieldNames(streamJoinConfig.joinFieldNamesRight);
    NES_DEBUG(
        "Created left and right memory provider for StreamJoin with page size {}--{}",
        leftMemoryProvider->getMemoryLayoutPtr()->getBufferSize(),
        rightMemoryProvider->getMemoryLayoutPtr()->getBufferSize());

    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore
        = std::make_unique<DefaultTimeBasedSliceStore>(streamJoinConfig.windowSize, streamJoinConfig.windowSlide, 2);
    return std::make_shared<Operators::NLJOperatorHandler>(
        joinOperator->getAllInputOriginIds(),
        joinOperator->getOutputOriginIds()[0],
        std::move(sliceAndWindowStore),
        leftMemoryProvider,
        rightMemoryProvider);
}

std::tuple<TimestampField, TimestampField> DefaultPhysicalOperatorProvider::getTimestampLeftAndRight(
    const std::shared_ptr<LogicalJoinOperator>& joinOperator, const Windowing::TimeBasedWindowTypePtr& windowType)
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
            for (const auto& field : *schema)
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
    PRECONDITION(
        not windowOperator->getInputOriginIds().empty(), "The number of input origin IDs for an window operator should not be zero.");

    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();

    auto timeBasedWindowType = NES::Util::as<Windowing::TimeBasedWindowType>(windowDefinition->getWindowType());
    auto windowOperatorProperties = WindowOperatorProperties(windowOperator, windowInputSchema, windowOutputSchema, windowDefinition);

    /// TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());
    windowDefinition->setInputOriginIds(windowOperator->getInputOriginIds());


    /// For now, we always use the default time beased slice store
    std::unique_ptr<Runtime::Execution::WindowSlicesStoreInterface> sliceAndWindowStore
        = std::make_unique<Runtime::Execution::DefaultTimeBasedSliceStore>(
            timeBasedWindowType->getSize().getTime(), timeBasedWindowType->getSlide().getTime(), 1);
    const auto windowHandler = std::make_shared<Runtime::Execution::Operators::AggregationOperatorHandler>(
        windowOperator->getInputOriginIds(), windowDefinition->getOriginId(), std::move(sliceAndWindowStore));

    Runtime::Execution::WindowMetaData const windowMetaData(
        windowOperator->getWindowStartFieldName(), windowOperator->getWindowEndFieldName());
    const auto aggregationBuild = PhysicalOperators::PhysicalAggregationBuild::create(
        getNextOperatorId(), windowInputSchema, windowOutputSchema, windowDefinition, windowHandler);
    const auto aggregationProbe = PhysicalOperators::PhysicalAggregationProbe::create(
        getNextOperatorId(), windowInputSchema, windowOutputSchema, windowDefinition, windowHandler, windowMetaData);
    operatorNode->insertBetweenThisAndChildNodes(aggregationBuild);
    operatorNode->replace(aggregationProbe);
}

void DefaultPhysicalOperatorProvider::lowerWindowOperator(const LogicalOperatorPtr& operatorNode)
{
    auto windowOperator = NES::Util::as<WindowOperator>(operatorNode);
    PRECONDITION(
        not windowOperator->getInputOriginIds().empty(), "The number of input origin IDs for an window operator should not be zero.");
    PRECONDITION(NES::Util::instanceOf<LogicalWindowOperator>(operatorNode), "The operator should be a window operator.");

    auto windowInputSchema = windowOperator->getInputSchema();
    auto windowOutputSchema = windowOperator->getOutputSchema();
    auto windowDefinition = windowOperator->getWindowDefinition();
    /// TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());

    /// handle if threshold window
    ///TODO: At this point we are already a central window, we do not want the threshold window to become a Gentral Window in the first place
    auto windowType = NES::Util::as<WindowOperator>(operatorNode)->getWindowDefinition()->getWindowType();
    if (NES::Util::instanceOf<Windowing::ContentBasedWindowType>(windowType))
    {
        auto contentBasedWindowType = NES::Util::as<Windowing::ContentBasedWindowType>(windowType);
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
    else if (NES::Util::instanceOf<Windowing::TimeBasedWindowType>(windowType))
    {
        lowerTimeBasedWindowOperator(operatorNode);
    }
}

}
