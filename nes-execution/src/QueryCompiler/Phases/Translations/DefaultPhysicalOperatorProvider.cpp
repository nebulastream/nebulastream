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
#include <ranges>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Execution/Operators/SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionCastToType.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
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
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalLimitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSortTuplesInBuffer.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationBuild.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationProbe.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/Common.hpp>
#include <Util/Execution.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::QueryCompilation
{

DefaultPhysicalOperatorProvider::DefaultPhysicalOperatorProvider(Configurations::QueryCompilerConfiguration queryCompilerConfig)
    : PhysicalOperatorProvider(std::move(queryCompilerConfig))
{
}

bool DefaultPhysicalOperatorProvider::isDemultiplex(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    return operatorNode->getParents().size() > 1;
}

void DefaultPhysicalOperatorProvider::insertDemultiplexOperatorsBefore(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    const auto operatorOutputSchema = operatorNode->getOutputSchema();
    /// A demultiplex operator has the same output schema as its child operator.
    const auto demultiplexOperator = PhysicalOperators::PhysicalDemultiplexOperator::create(operatorOutputSchema);
    demultiplexOperator->setOutputSchema(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndParentNodes(demultiplexOperator);
}

void DefaultPhysicalOperatorProvider::insertMultiplexOperatorsAfter(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    /// the unionOperator operator has the same schema as the output schema of the operator node.
    const auto unionOperator = PhysicalOperators::PhysicalUnionOperator::create(operatorNode->getOutputSchema());
    operatorNode->insertBetweenThisAndChildNodes(unionOperator);
}

void DefaultPhysicalOperatorProvider::lower(
    const DecomposedQueryPlan& decomposedQueryPlan, const std::shared_ptr<LogicalOperator> operatorNode)
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

    NES_DEBUG("DefaultPhysicalOperatorProvider:: Plan after lowering \n{}", decomposedQueryPlan.toString());
}

void DefaultPhysicalOperatorProvider::lowerUnaryOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    PRECONDITION(operatorNode->getParents().size() <= 1, "A unary operator should have at most one parent.");

    /// If a unary operator has more than one parent, we introduce an implicit multiplex operator before.
    if (operatorNode->getChildren().size() > 1)
    {
        insertMultiplexOperatorsAfter(operatorNode);
    }

    if (NES::Util::instanceOf<LogicalSelectionOperator>(operatorNode))
    {
        const auto filterOperator = NES::Util::as<LogicalSelectionOperator>(operatorNode);
        const auto physicalSelectionOperator = PhysicalOperators::PhysicalSelectionOperator::create(
            filterOperator->getInputSchema(), filterOperator->getOutputSchema(), filterOperator->getPredicate());
        physicalSelectionOperator->addProperty("LogicalOperatorId", operatorNode->getId());
        operatorNode->replace(physicalSelectionOperator);
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
    else if (NES::Util::instanceOf<LogicalProjectionOperator>(operatorNode))
    {
        lowerProjectOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<LogicalLimitOperator>(operatorNode))
    {
        const auto limitOperator = NES::Util::as<LogicalLimitOperator>(operatorNode);
        const auto physicalLimitOperator = PhysicalOperators::PhysicalLimitOperator::create(
            limitOperator->getInputSchema(), limitOperator->getOutputSchema(), limitOperator->getLimit());
        operatorNode->replace(physicalLimitOperator);
    }
    else
    {
        throw UnknownLogicalOperator("Operator: {}", *operatorNode);
    }
}

void DefaultPhysicalOperatorProvider::lowerBinaryOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    PRECONDITION(
        NES::Util::instanceOf<LogicalUnionOperator>(operatorNode) || NES::Util::instanceOf<LogicalJoinOperator>(operatorNode),
        "{} is no binaryOperator",
        *operatorNode);
    if (NES::Util::instanceOf<LogicalUnionOperator>(operatorNode))
    {
        lowerUnionOperator(operatorNode);
    }
    else if (NES::Util::instanceOf<LogicalJoinOperator>(operatorNode))
    {
        lowerJoinOperator(operatorNode);
    }
}

void DefaultPhysicalOperatorProvider::lowerUnionOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    const auto unionOperator = NES::Util::as<LogicalUnionOperator>(operatorNode);
    /// this assumes that we apply the ProjectBeforeUnionRule and the input across all children is the same.
    PRECONDITION(
        *unionOperator->getLeftInputSchema() == *unionOperator->getRightInputSchema(),
        "The input schemas of a union operators children should be equal");

    const auto physicalUnionOperator = PhysicalOperators::PhysicalUnionOperator::create(unionOperator->getLeftInputSchema());
    operatorNode->replace(physicalUnionOperator);
}

void DefaultPhysicalOperatorProvider::lowerProjectOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    const auto projectOperator = NES::Util::as<LogicalProjectionOperator>(operatorNode);
    const auto physicalProjectOperator = PhysicalOperators::PhysicalProjectOperator::create(
        projectOperator->getInputSchema(), projectOperator->getOutputSchema(), projectOperator->getFunctions());

    physicalProjectOperator->addProperty("LogicalOperatorId", projectOperator->getId());
    operatorNode->replace(physicalProjectOperator);
}

void DefaultPhysicalOperatorProvider::lowerMapOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    const auto mapOperator = NES::Util::as<LogicalMapOperator>(operatorNode);
    const auto physicalMapOperator = PhysicalOperators::PhysicalMapOperator::create(
        mapOperator->getInputSchema(), mapOperator->getOutputSchema(), mapOperator->getMapFunction());
    physicalMapOperator->addProperty("LogicalOperatorId", operatorNode->getId());
    operatorNode->replace(physicalMapOperator);
}

std::shared_ptr<Operator> DefaultPhysicalOperatorProvider::getJoinBuildInputOperator(
    const std::shared_ptr<LogicalJoinOperator>& joinOperator,
    const std::shared_ptr<Schema>& outputSchema,
    std::vector<std::shared_ptr<Operator>> children)
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

void DefaultPhysicalOperatorProvider::lowerJoinOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    using namespace Runtime::Execution::Operators;

    const auto joinOperator = NES::Util::as<LogicalJoinOperator>(operatorNode);
    const auto& joinDefinition = joinOperator->getJoinDefinition();

    auto getJoinFieldNamesLeftRight = [](const Schema& leftInputSchema, const Schema& rightInputSchema, NodeFunction& joinFunction)
        -> std::pair<std::vector<PhysicalOperators::FieldNamesExtension>, std::vector<PhysicalOperators::FieldNamesExtension>>
    {
        /// Tuple  of left, right join fields and the combined data type, e.g., i32 and i8 --> i32
        std::vector<PhysicalOperators::FieldNamesExtension> leftJoinNames;
        std::vector<PhysicalOperators::FieldNamesExtension> rightJoinNames;
        std::set<std::shared_ptr<Node>> visitedParents;
        uint64_t counter = 0;
        std::ranges::for_each(
            joinFunction.getAllLeafNodes(),
            [leftInputSchema, rightInputSchema, &leftJoinNames, &rightJoinNames, &counter, &visitedParents](const auto& leaf)
            {
                DefaultPhysicalTypeFactory typeFactory;
                /// We expect a leaf has a parent that is a binary node for a join function.
                if (const auto fieldNodeAccessLeaf = dynamic_cast<NodeFunctionFieldAccess*>(leaf.get()))
                {
                    const auto parent = fieldNodeAccessLeaf->getParents()[0];
                    if (not visitedParents.emplace(parent).second)
                    {
                        /// We have already seen/visited the node
                        return;
                    }

                    const auto children = NES::Util::as<NodeFunction>(parent)->getChildren();
                    std::optional<std::shared_ptr<AttributeField>> leftField, rightField;
                    std::optional<std::shared_ptr<NodeFunctionFieldAccess>> leftFieldAccess, rightFieldAccess;
                    for (const auto& child : children)
                    {
                        const auto fieldName = NES::Util::as<NodeFunctionFieldAccess>(child)->getFieldName();
                        leftField = leftField.has_value() ? leftField : leftInputSchema.getFieldByName(fieldName);
                        rightField = rightField.has_value() ? rightField : rightInputSchema.getFieldByName(fieldName);
                        if (not leftFieldAccess.has_value())
                        {
                            if (leftField.has_value())
                            {
                                leftFieldAccess = NES::Util::as<NodeFunctionFieldAccess>(child);
                            }
                        }
                        if (not rightFieldAccess.has_value())
                        {
                            if (rightField.has_value())
                            {
                                rightFieldAccess = NES::Util::as<NodeFunctionFieldAccess>(child);
                            }
                        }
                    }
                    INVARIANT(leftField.has_value() and rightField.has_value(), "Could not find left and right leaves");
                    INVARIANT(leftFieldAccess.has_value() and rightFieldAccess.has_value(), "Could not find left and right leaves field access");

                    /// We are now converting the fields to a physical data type and then joining them together
                    const auto joinedDataType = leftField.value()->getDataType()->join(rightField.value()->getDataType());
                    const auto leftFieldNewName = leftField.value()->getName() + "_" + std::to_string(counter++);
                    const auto rightFieldNewName = rightField.value()->getName() + "_" + std::to_string(counter++);
                    leftFieldAccess.value()->updateFieldName(leftFieldNewName);
                    rightFieldAccess.value()->updateFieldName(rightFieldNewName);
                    leftJoinNames.emplace_back(
                        PhysicalOperators::FieldNamesExtension{
                            leftField.value()->getName(),
                            leftFieldNewName,
                            leftField.value()->getDataType(),
                            joinedDataType});
                    rightJoinNames.emplace_back(
                        PhysicalOperators::FieldNamesExtension{
                            rightField.value()->getName(),
                            rightFieldNewName,
                            rightField.value()->getDataType(),
                            joinedDataType});
                }
            });

        return {leftJoinNames, rightJoinNames};
    };

    /// We have now access to the mapping of old to new fields. So basically what fields do we need to change so that the join gets all
    /// fields with the correct data type.
    const auto& [leftJoinFields, rightJoinFields] = getJoinFieldNamesLeftRight(
        *joinDefinition->getLeftSourceType(), *joinDefinition->getRightSourceType(), *joinDefinition->getJoinFunction());
    const auto leftFieldNamesView
        = leftJoinFields | std::views::transform([](const PhysicalOperators::FieldNamesExtension& item) { return item.newName; });
    const auto rightFieldNamesView
        = rightJoinFields | std::views::transform([](const PhysicalOperators::FieldNamesExtension& item) { return item.newName; });
    std::vector<std::string> leftJoinFieldNames(leftFieldNamesView.begin(), leftFieldNamesView.end());
    std::vector<std::string> rightJoinFieldNames(rightFieldNamesView.begin(), rightFieldNamesView.end());


    const auto windowType = NES::Util::as<Windowing::TimeBasedWindowType>(joinDefinition->getWindowType());
    const auto& windowSize = windowType->getSize().getTime();
    const auto& windowSlide = windowType->getSlide().getTime();
    INVARIANT(
        NES::Util::instanceOf<Windowing::TumblingWindow>(windowType) || NES::Util::instanceOf<Windowing::SlidingWindow>(windowType),
        "Only a tumbling or sliding window is currently supported for StreamJoin, but got: {}",
        windowType->toString());

    const auto [timeStampFieldLeft, timeStampFieldRight] = getTimestampLeftAndRight(joinOperator, windowType);
    auto leftInputOperator = getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
    auto rightInputOperator
        = getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());
    const auto joinStrategy = queryCompilerConfig.joinStrategy;


    /// Now we need to create for each field a map operator that has as its function a cast to the correct data type
    auto addMapOperators = [](const std::shared_ptr<Schema>& inputSchemaOfJoin,
                              const std::vector<PhysicalOperators::FieldNamesExtension>& fieldNameExtensions,
                              const std::shared_ptr<NES::Operator>& inputOperator)
    {
        const std::shared_ptr<Schema> inputSchemaOfMap = Schema::create(inputSchemaOfJoin->getLayoutType());
        inputSchemaOfMap->copyFields(inputSchemaOfJoin);
        auto workingCopyOfInputOperator = inputOperator;
        for (const auto& [oldName, newName, oldDataType, newDataType] : fieldNameExtensions)
        {
            auto nodeFieldAccessOldField = NodeFunctionFieldAccess::create(oldDataType, oldName);
            auto nodeFieldAccessNewField = NodeFunctionFieldAccess::create(newDataType, newName);
            auto nodeFunctionCast = NodeFunctionCastToType::create(newDataType);
            nodeFunctionCast->addChild(nodeFieldAccessOldField);
            auto nodeFunctionFieldAssignment
                = NodeFunctionFieldAssignment::create(NES::Util::as<NodeFunctionFieldAccess>(nodeFieldAccessNewField), nodeFunctionCast);

            /// Get a copy of the current input schema
            const auto copyOfInputSchemaOfMap = Schema::create(inputSchemaOfJoin->getLayoutType());
            copyOfInputSchemaOfMap->copyFields(inputSchemaOfMap);
            /// Add to the inputSchemaOfMap the newly added fields
            inputSchemaOfMap->addField(newName, newDataType);

            /// Create a new map operator with the cast as its function and add it before the join operator
            auto physicalMap
                = PhysicalOperators::PhysicalMapOperator::create(copyOfInputSchemaOfMap, inputSchemaOfMap, nodeFunctionFieldAssignment);
            workingCopyOfInputOperator->insertBetweenThisAndParentNodes(physicalMap);
            workingCopyOfInputOperator = physicalMap;
        }
        return inputSchemaOfMap;
    };
    auto newLeftInputSchema = addMapOperators(joinOperator->getLeftInputSchema(), leftJoinFields, leftInputOperator);
    auto newRightInputSchema = addMapOperators(joinOperator->getRightInputSchema(), rightJoinFields, rightInputOperator);
    joinOperator->getLeftInputSchema()->clear();
    joinOperator->getRightInputSchema()->clear();
    joinOperator->getLeftInputSchema()->copyFields(newLeftInputSchema);
    joinOperator->getRightInputSchema()->copyFields(newRightInputSchema);


    leftInputOperator = getJoinBuildInputOperator(joinOperator, joinOperator->getLeftInputSchema(), joinOperator->getLeftOperators());
    rightInputOperator = getJoinBuildInputOperator(joinOperator, joinOperator->getRightInputSchema(), joinOperator->getRightOperators());
    StreamJoinOperators streamJoinOperators(operatorNode, leftInputOperator, rightInputOperator);
    const StreamJoinConfigs streamJoinConfig(
        leftJoinFieldNames, rightJoinFieldNames, windowSize, windowSlide, timeStampFieldLeft, timeStampFieldRight, joinStrategy);

    std::shared_ptr<StreamJoinOperatorHandler> joinOperatorHandler;
    switch (joinStrategy)
    {
        case Configurations::StreamJoinStrategy::NESTED_LOOP_JOIN:
            joinOperatorHandler = lowerStreamingNestedLoopJoin(streamJoinOperators, streamJoinConfig);
            break;
        case Configurations::StreamJoinStrategy::HASH_JOIN: {
            auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
                queryCompilerConfig.pageSize.getValue(), joinOperator->getLeftInputSchema());
            leftMemoryProvider->getMemoryLayout()->setKeyFieldNames(streamJoinConfig.joinFieldNamesLeft);
            auto rightMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
                queryCompilerConfig.pageSize.getValue(), joinOperator->getRightInputSchema());
            rightMemoryProvider->getMemoryLayout()->setKeyFieldNames(streamJoinConfig.joinFieldNamesRight);
            NES_DEBUG(
                "Created left and right memory provider for StreamJoin with page size {}--{}",
                leftMemoryProvider->getMemoryLayout()->getBufferSize(),
                rightMemoryProvider->getMemoryLayout()->getBufferSize());

            std::unique_ptr<Runtime::Execution::WindowSlicesStoreInterface> sliceAndWindowStore
                = std::make_unique<Runtime::Execution::DefaultTimeBasedSliceStore>(
                    streamJoinConfig.windowSize, streamJoinConfig.windowSlide, joinOperator->getAllInputOriginIds().size());
            joinOperatorHandler = std::make_shared<HJOperatorHandler>(
                joinOperator->getAllInputOriginIds(),
                joinOperator->getOutputOriginIds()[0],
                std::move(sliceAndWindowStore),
                leftMemoryProvider,
                rightMemoryProvider);
            break;
        }
            std::unreachable();
    }

    auto createBuildOperator = [&](const std::shared_ptr<Schema>& inputSchema,
                                   const std::vector<PhysicalOperators::FieldNamesExtension>& joinFields,
                                   JoinBuildSideType buildSideType,
                                   const TimestampField& timeStampField)
    {
        return std::make_shared<PhysicalOperators::PhysicalStreamJoinBuildOperator>(
            inputSchema,
            joinOperator->getOutputSchema(),
            joinOperatorHandler,
            queryCompilerConfig.joinStrategy,
            joinFields,
            timeStampField,
            buildSideType);
    };
    auto joinFunctionLowered = FunctionProvider::lowerFunction(joinOperator->getJoinFunction());
    const auto leftJoinBuildOperator = createBuildOperator(
        joinOperator->getLeftInputSchema(), leftJoinFields, JoinBuildSideType::Left, streamJoinConfig.timeStampFieldLeft);
    const auto rightJoinBuildOperator = createBuildOperator(
        joinOperator->getRightInputSchema(), rightJoinFields, JoinBuildSideType::Right, streamJoinConfig.timeStampFieldRight);
    const auto joinProbeOperator = std::make_shared<PhysicalOperators::PhysicalStreamJoinProbeOperator>(
        joinOperator->getLeftInputSchema(),
        joinOperator->getRightInputSchema(),
        joinOperator->getOutputSchema(),
        joinOperatorHandler,
        queryCompilerConfig.joinStrategy,
        std::move(joinFunctionLowered),
        streamJoinConfig.joinFieldNamesLeft,
        streamJoinConfig.joinFieldNamesRight,
        joinOperator->windowMetaData);
    if (queryCompilerConfig.sortBufferByTimestamp.getValue())
    {
        const auto sortTuplesInBufferLeft = std::make_shared<PhysicalOperators::PhysicalSortTuplesInBuffer>(
            getNextOperatorId(), joinOperator->getLeftInputSchema(), joinOperator->getLeftInputSchema(), timeStampFieldLeft.getName());

        const auto sortTuplesInBufferRight = std::make_shared<PhysicalOperators::PhysicalSortTuplesInBuffer>(
            getNextOperatorId(), joinOperator->getRightInputSchema(), joinOperator->getRightInputSchema(), timeStampFieldRight.getName());
        streamJoinOperators.leftInputOperator->insertBetweenThisAndParentNodes(sortTuplesInBufferLeft);
        streamJoinOperators.rightInputOperator->insertBetweenThisAndParentNodes(sortTuplesInBufferRight);
        streamJoinOperators.leftInputOperator = sortTuplesInBufferLeft;
        streamJoinOperators.rightInputOperator = sortTuplesInBufferRight;
    }

    streamJoinOperators.leftInputOperator->insertBetweenThisAndParentNodes(leftJoinBuildOperator);
    streamJoinOperators.rightInputOperator->insertBetweenThisAndParentNodes(rightJoinBuildOperator);
    streamJoinOperators.operatorNode->replace(joinProbeOperator);
}

std::shared_ptr<Runtime::Execution::Operators::StreamJoinOperatorHandler> DefaultPhysicalOperatorProvider::lowerStreamingNestedLoopJoin(
    const StreamJoinOperators& streamJoinOperators, const StreamJoinConfigs& streamJoinConfig) const
{
    using namespace Runtime::Execution;
    const auto joinOperator = NES::Util::as<LogicalJoinOperator>(streamJoinOperators.operatorNode);

    auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        queryCompilerConfig.pageSize.getValue(), joinOperator->getLeftInputSchema());
    leftMemoryProvider->getMemoryLayout()->setKeyFieldNames(streamJoinConfig.joinFieldNamesLeft);
    auto rightMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
        queryCompilerConfig.pageSize.getValue(), joinOperator->getRightInputSchema());
    rightMemoryProvider->getMemoryLayout()->setKeyFieldNames(streamJoinConfig.joinFieldNamesRight);
    NES_DEBUG(
        "Created left and right memory provider for StreamJoin with page size {}--{}",
        leftMemoryProvider->getMemoryLayout()->getBufferSize(),
        rightMemoryProvider->getMemoryLayout()->getBufferSize());

    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore = std::make_unique<DefaultTimeBasedSliceStore>(
        streamJoinConfig.windowSize, streamJoinConfig.windowSlide, joinOperator->getAllInputOriginIds().size());
    return std::make_shared<Operators::NLJOperatorHandler>(
        joinOperator->getAllInputOriginIds(),
        joinOperator->getOutputOriginIds()[0],
        std::move(sliceAndWindowStore),
        leftMemoryProvider,
        rightMemoryProvider);
}

std::tuple<TimestampField, TimestampField> DefaultPhysicalOperatorProvider::getTimestampLeftAndRight(
    const std::shared_ptr<LogicalJoinOperator>& joinOperator, const std::shared_ptr<Windowing::TimeBasedWindowType>& windowType)
{
    if (windowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
    {
        NES_DEBUG("Skip eventime identification as we use ingestion time");
        return {TimestampField::IngestionTime(), TimestampField::IngestionTime()};
    }
    else
    {
        /// FIXME Once #3407 is done, we can change this to get the left and right fieldname
        auto timeStampFieldName = windowType->getTimeCharacteristic()->field->getName();
        auto timeStampFieldNameWithoutSourceName = timeStampFieldName.substr(timeStampFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR));

        /// Lambda function for extracting the timestamp from a schema
        auto findTimeStampFieldName = [&](const std::shared_ptr<Schema>& schema)
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

        INVARIANT(
            !(timeStampFieldNameLeft.empty() || timeStampFieldNameRight.empty()),
            "Could not find timestampfieldname {} in both streams!",
            timeStampFieldNameWithoutSourceName);
        NES_DEBUG("timeStampFieldNameLeft:{}  timeStampFieldNameRight:{} ", timeStampFieldNameLeft, timeStampFieldNameRight);

        return {
            TimestampField::EventTime(timeStampFieldNameLeft, windowType->getTimeCharacteristic()->getTimeUnit()),
            TimestampField::EventTime(timeStampFieldNameRight, windowType->getTimeCharacteristic()->getTimeUnit())};
    }
}

void DefaultPhysicalOperatorProvider::lowerWatermarkAssignmentOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    const auto logicalWatermarkAssignment = NES::Util::as<WatermarkAssignerLogicalOperator>(operatorNode);
    const auto physicalWatermarkAssignment = PhysicalOperators::PhysicalWatermarkAssignmentOperator::create(
        logicalWatermarkAssignment->getInputSchema(),
        logicalWatermarkAssignment->getOutputSchema(),
        logicalWatermarkAssignment->getWatermarkStrategyDescriptor());
    physicalWatermarkAssignment->setOutputSchema(logicalWatermarkAssignment->getOutputSchema());
    operatorNode->replace(physicalWatermarkAssignment);
}

void DefaultPhysicalOperatorProvider::lowerTimeBasedWindowOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    NES_DEBUG("Create Thread local window aggregation");
    const auto windowOperator = NES::Util::as<WindowOperator>(operatorNode);
    PRECONDITION(
        not windowOperator->getInputOriginIds().empty(), "The number of input origin IDs for an window operator should not be zero.");

    const auto windowInputSchema = windowOperator->getInputSchema();
    const auto windowOutputSchema = windowOperator->getOutputSchema();
    const auto windowDefinition = windowOperator->getWindowDefinition();

    const auto timeBasedWindowType = NES::Util::as<Windowing::TimeBasedWindowType>(windowDefinition->getWindowType());
    auto windowOperatorProperties = WindowOperatorProperties(windowOperator, windowInputSchema, windowOutputSchema, windowDefinition);

    /// TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());
    windowDefinition->setInputOriginIds(windowOperator->getInputOriginIds());


    /// For now, we always use the default time beased slice store
    const auto numberOfInputOrigins = windowOperator->getInputOriginIds().size();
    std::unique_ptr<Runtime::Execution::WindowSlicesStoreInterface> sliceAndWindowStore
        = std::make_unique<Runtime::Execution::DefaultTimeBasedSliceStore>(
            timeBasedWindowType->getSize().getTime(), timeBasedWindowType->getSlide().getTime(), numberOfInputOrigins);
    const auto windowHandler = std::make_shared<Runtime::Execution::Operators::AggregationOperatorHandler>(
        windowOperator->getInputOriginIds(), windowDefinition->getOriginId(), std::move(sliceAndWindowStore));

    const auto aggregationBuild = PhysicalOperators::PhysicalAggregationBuild::create(
        getNextOperatorId(), windowInputSchema, windowOutputSchema, windowDefinition, windowHandler);
    const auto aggregationProbe = PhysicalOperators::PhysicalAggregationProbe::create(
        getNextOperatorId(), windowInputSchema, windowOutputSchema, windowDefinition, windowHandler, windowOperator->windowMetaData);
    operatorNode->insertBetweenThisAndChildNodes(aggregationBuild);
    operatorNode->replace(aggregationProbe);
    if (queryCompilerConfig.sortBufferByTimestamp.getValue())
    {
        const auto sortTuplesInBuffer = std::make_shared<PhysicalOperators::PhysicalSortTuplesInBuffer>(
            getNextOperatorId(), windowInputSchema, windowInputSchema, timeBasedWindowType->getTimeCharacteristic()->field->getName());
        aggregationBuild->insertBetweenThisAndChildNodes(sortTuplesInBuffer);
    }
}

void DefaultPhysicalOperatorProvider::lowerWindowOperator(const std::shared_ptr<LogicalOperator>& operatorNode)
{
    const auto windowOperator = NES::Util::as<WindowOperator>(operatorNode);
    PRECONDITION(
        not windowOperator->getInputOriginIds().empty(), "The number of input origin IDs for an window operator should not be zero.");
    PRECONDITION(NES::Util::instanceOf<LogicalWindowOperator>(operatorNode), "The operator should be a window operator.");

    const auto windowInputSchema = windowOperator->getInputSchema();
    const auto windowOutputSchema = windowOperator->getOutputSchema();
    const auto windowDefinition = windowOperator->getWindowDefinition();
    /// TODO this currently just mimics the old usage of the set of input origins.
    windowDefinition->setNumberOfInputEdges(windowOperator->getInputOriginIds().size());

    /// handle if threshold window
    ///TODO: At this point we are already a central window, we do not want the threshold window to become a Gentral Window in the first place
    const auto windowType = NES::Util::as<WindowOperator>(operatorNode)->getWindowDefinition()->getWindowType();
    if (NES::Util::instanceOf<Windowing::ContentBasedWindowType>(windowType))
    {
        const auto contentBasedWindowType = NES::Util::as<Windowing::ContentBasedWindowType>(windowType);
        /// check different content-based window types
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            NES_INFO("Lower ThresholdWindow");
            const auto thresholdWindowPhysicalOperator
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
