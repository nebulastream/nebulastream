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

#include <cstddef>
#include <string_view>
#include <utility>
#include <API/Schema.hpp>
#include <API/TimeUnit.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/IngestionTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbe.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbeVarSized.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJBuildSlicingVarSized.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindowOperatorHandler.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalLimitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/ExpressionProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/Core.hpp>
#include <Util/Execution.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>

namespace NES::QueryCompilation
{

LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators(std::shared_ptr<QueryCompilerOptions> options)
    : options(std::move(options)), expressionProvider(std::make_unique<ExpressionProvider>())
{
}

PipelineQueryPlanPtr LowerPhysicalToNautilusOperators::apply(PipelineQueryPlanPtr pipelinedQueryPlan, size_t bufferSize)
{
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines())
    {
        if (pipeline->isOperatorPipeline())
        {
            apply(pipeline, bufferSize);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr LowerPhysicalToNautilusOperators::apply(OperatorPipelinePtr operatorPipeline, size_t bufferSize)
{
    auto decomposedQueryPlan = operatorPipeline->getDecomposedQueryPlan();
    auto nodes = PlanIterator(decomposedQueryPlan).snapshot();
    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;
    std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator;

    for (const auto& node : nodes)
    {
        NES_INFO("Lowering node: {}", node->toString());
        parentOperator
            = lower(*pipeline, parentOperator, NES::Util::as<PhysicalOperators::PhysicalOperator>(node), bufferSize, operatorHandlers);
    }
    const auto& rootOperators = decomposedQueryPlan->getRootOperators();
    for (auto& root : rootOperators)
    {
        decomposedQueryPlan->removeAsRootOperator(root->getId());
    }
    auto nautilusPipelineWrapper = NautilusPipelineOperator::create(pipeline, operatorHandlers);
    decomposedQueryPlan->addRootOperator(nautilusPipelineWrapper);
    return operatorPipeline;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lower(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
    size_t bufferSize,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    NES_INFO("Lower node:{} to NautilusOperator.", operatorNode->toString());
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalScanOperator>(operatorNode))
    {
        auto scan = lowerScan(pipeline, operatorNode, bufferSize);
        pipeline.setRootOperator(scan);
        return scan;
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalEmitOperator>(operatorNode))
    {
        auto emit = lowerEmit(pipeline, operatorNode, bufferSize);
        parentOperator->setChild(emit);
        return emit;
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalFilterOperator>(operatorNode))
    {
        auto filter = lowerFilter(pipeline, operatorNode);
        parentOperator->setChild(filter);
        return filter;
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalMapOperator>(operatorNode))
    {
        auto map = lowerMap(pipeline, operatorNode);
        parentOperator->setChild(map);
        return map;
    }

    throw UnknownPhysicalOperator();
}

Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerNLJSlicing(
    std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator> buildOperator,
    uint64_t operatorHandlerIndex,
    Runtime::Execution::Operators::TimeFunctionPtr timeFunction)
{
    return std::make_shared<Runtime::Execution::Operators::NLJBuildSlicing>(
        operatorHandlerIndex,
        buildOperator->getInputSchema(),
        buildOperator->getJoinFieldName(),
        buildOperator->getBuildSide(),
        buildOperator->getInputSchema()->getSchemaSizeInBytes(),
        std::move(timeFunction),
        buildOperator->getJoinStrategy());
}

Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerHJSlicing(
    const std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator>& buildOperator,
    uint64_t operatorHandlerIndex,
    Runtime::Execution::Operators::TimeFunctionPtr timeFunction)
{
    return std::make_shared<Runtime::Execution::Operators::HJBuildSlicing>(
        operatorHandlerIndex,
        buildOperator->getInputSchema(),
        buildOperator->getJoinFieldName(),
        buildOperator->getBuildSide(),
        buildOperator->getInputSchema()->getSchemaSizeInBytes(),
        std::move(timeFunction),
        buildOperator->getJoinStrategy());
}

Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerHJSlicingVarSized(
    const std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator>& buildOperator,
    uint64_t operatorHandlerIndex,
    Runtime::Execution::Operators::TimeFunctionPtr timeFunction)
{
    return std::make_shared<Runtime::Execution::Operators::HJBuildSlicingVarSized>(
        operatorHandlerIndex,
        buildOperator->getInputSchema(),
        buildOperator->getJoinFieldName(),
        buildOperator->getBuildSide(),
        buildOperator->getInputSchema()->getSchemaSizeInBytes(),
        std::move(timeFunction),
        buildOperator->getJoinStrategy());
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerWindowSinkOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto sinkOperator = NES::Util::as<PhysicalOperators::PhysicalWindowSinkOperator>(physicalOperator);
    if (sinkOperator->getWindowDefinition()->isKeyed())
    {
        return lowerKeyedWindowSinkOperator(pipeline, physicalOperator, operatorHandlers);
    }
    else
    {
        return lowerNonKeyedWindowSinkOperator(pipeline, physicalOperator, operatorHandlers);
    }
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerKeyedWindowSinkOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto physicalSWS = NES::Util::as<PhysicalOperators::PhysicalWindowSinkOperator>(physicalOperator);

    auto aggregations = physicalSWS->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    /// We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    /// TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalSWS->getOutputSchema()->get(0)->getName();
    auto endTs = physicalSWS->getOutputSchema()->get(1)->getName();
    auto windowType = physicalSWS->getWindowDefinition()->getWindowType();
    auto keys = physicalSWS->getWindowDefinition()->getKeys();
    std::vector<std::string> resultKeyFields;
    std::vector<PhysicalTypePtr> resultKeyDataTypes;
    for (const auto& key : keys)
    {
        resultKeyFields.emplace_back(key->getFieldName());
        resultKeyDataTypes.emplace_back(DefaultPhysicalTypeFactory().getPhysicalType(key->getStamp()));
    }
    uint64_t keySize = 0;
    uint64_t valueSize = 0;
    for (auto& keyType : resultKeyDataTypes)
    {
        keySize = keySize + keyType->size();
    }
    for (auto& function : aggregationFunctions)
    {
        valueSize = valueSize + function->getSize();
    }

    std::unique_ptr<Runtime::Execution::Operators::SliceMergingAction> sliceMergingAction
        = std::make_unique<Runtime::Execution::Operators::KeyedWindowEmitAction>(
            aggregationFunctions,
            startTs,
            endTs,
            keySize,
            valueSize,
            resultKeyFields,
            resultKeyDataTypes,
            physicalSWS->getWindowDefinition()->getOriginId());
    auto handler = std::make_shared<Runtime::Execution::Operators::KeyedSliceMergingHandler>();
    operatorHandlers.emplace_back(handler);
    auto sliceMergingOperator = std::make_shared<Runtime::Execution::Operators::KeyedSliceMerging>(
        operatorHandlers.size() - 1, aggregationFunctions, std::move(sliceMergingAction), resultKeyDataTypes, keySize, valueSize);

    pipeline.setRootOperator(sliceMergingOperator);
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerNonKeyedWindowSinkOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto physicalSWS = NES::Util::as<PhysicalOperators::PhysicalWindowSinkOperator>(physicalOperator);

    auto aggregations = physicalSWS->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    /// We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    /// TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalSWS->getOutputSchema()->get(0)->getName();
    auto endTs = physicalSWS->getOutputSchema()->get(1)->getName();
    auto windowType = physicalSWS->getWindowDefinition()->getWindowType();
    std::unique_ptr<Runtime::Execution::Operators::SliceMergingAction> sliceMergingAction
        = std::make_unique<Runtime::Execution::Operators::NonKeyedWindowEmitAction>(
            aggregationFunctions, startTs, endTs, physicalSWS->getWindowDefinition()->getOriginId());

    auto handler = std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMergingHandler>();
    operatorHandlers.emplace_back(handler);
    auto nonKeyedSliceMergingOperator = std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMerging>(
        operatorHandlers.size() - 1, aggregationFunctions, std::move(sliceMergingAction));

    pipeline.setRootOperator(nonKeyedSliceMergingOperator);
    return nonKeyedSliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto sliceMerging = NES::Util::as<PhysicalOperators::PhysicalSliceMergingOperator>(physicalOperator);
    if (sliceMerging->getWindowDefinition()->isKeyed())
    {
        return lowerKeyedSliceMergingOperator(pipeline, physicalOperator, operatorHandlers);
    }
    else
    {
        return lowerNonKeyedSliceMergingOperator(pipeline, physicalOperator, operatorHandlers);
    }
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerNonKeyedSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto physicalGSMO = NES::Util::as<PhysicalOperators::PhysicalSliceMergingOperator>(physicalOperator);
    auto handler = std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMergingHandler>();
    operatorHandlers.emplace_back(handler);
    auto sliceMergingOperatorHandlerIndex = operatorHandlers.size() - 1;
    auto aggregations = physicalGSMO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    /// We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    /// TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.

    /// TODO refactor operator selection
    auto windowType = physicalGSMO->getWindowDefinition()->getWindowType();
    auto isTumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType) != nullptr ? true : false;
    auto startTs = physicalGSMO->getOutputSchema()->get(0)->getName();
    auto endTs = physicalGSMO->getOutputSchema()->get(1)->getName();

    std::unique_ptr<Runtime::Execution::Operators::SliceMergingAction> sliceMergingAction;
    if (isTumblingWindow)
    {
        sliceMergingAction = std::make_unique<Runtime::Execution::Operators::NonKeyedWindowEmitAction>(
            aggregationFunctions, startTs, endTs, physicalGSMO->getWindowDefinition()->getOriginId());
    }
    else
    {
        const auto timeBasedWindowType
            = NES::Util::as<Windowing::TimeBasedWindowType>(physicalGSMO->getWindowDefinition()->getWindowType());
        const auto& [windowSize, windowSlide, _] = Util::getWindowingParameters(*timeBasedWindowType);
        auto actionHandler = std::make_shared<Runtime::Execution::Operators::NonKeyedAppendToSliceStoreHandler>(windowSize, windowSlide);
        operatorHandlers.emplace_back(actionHandler);
        sliceMergingAction = std::make_unique<Runtime::Execution::Operators::NonKeyedAppendToSliceStoreAction>(operatorHandlers.size() - 1);
    }
    auto nonKeyedSliceMergingOperator = std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMerging>(
        sliceMergingOperatorHandlerIndex, aggregationFunctions, std::move(sliceMergingAction));

    pipeline.setRootOperator(nonKeyedSliceMergingOperator);
    return nonKeyedSliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerKeyedSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto physicalGSMO = NES::Util::as<PhysicalOperators::PhysicalSliceMergingOperator>(physicalOperator);

    auto handler = std::make_shared<Runtime::Execution::Operators::KeyedSliceMergingHandler>();

    operatorHandlers.emplace_back(handler);
    auto sliceMergingOperatorHandlerIndex = operatorHandlers.size() - 1;
    auto aggregations = physicalGSMO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    /// We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    /// TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalGSMO->getOutputSchema()->get(0)->getName();
    auto endTs = physicalGSMO->getOutputSchema()->get(1)->getName();
    auto keys = physicalGSMO->getWindowDefinition()->getKeys();

    std::vector<std::string> resultKeyFields;
    std::vector<PhysicalTypePtr> resultKeyDataTypes;
    for (const auto& key : keys)
    {
        resultKeyFields.emplace_back(key->getFieldName());
        resultKeyDataTypes.emplace_back(DefaultPhysicalTypeFactory().getPhysicalType(key->getStamp()));
    }
    uint64_t keySize = 0;
    uint64_t valueSize = 0;
    for (auto& keyType : resultKeyDataTypes)
    {
        keySize = keySize + keyType->size();
    }
    for (auto& function : aggregationFunctions)
    {
        valueSize = valueSize + function->getSize();
    }
    auto windowType = physicalGSMO->getWindowDefinition()->getWindowType();
    auto isTumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType) != nullptr ? true : false;

    std::unique_ptr<Runtime::Execution::Operators::SliceMergingAction> sliceMergingAction;
    if (isTumblingWindow)
    {
        sliceMergingAction = std::make_unique<Runtime::Execution::Operators::KeyedWindowEmitAction>(
            aggregationFunctions,
            startTs,
            endTs,
            keySize,
            valueSize,
            resultKeyFields,
            resultKeyDataTypes,
            physicalGSMO->getWindowDefinition()->getOriginId());
    }
    else
    {
        const auto windowType = NES::Util::as<Windowing::TimeBasedWindowType>(physicalGSMO->getWindowDefinition()->getWindowType());
        const auto& [windowSize, windowSlide, timeFunction] = Util::getWindowingParameters(*windowType);
        auto actionHandler = std::make_shared<Runtime::Execution::Operators::KeyedAppendToSliceStoreHandler>(windowSize, windowSlide);
        operatorHandlers.emplace_back(actionHandler);
        sliceMergingAction = std::make_unique<Runtime::Execution::Operators::KeyedAppendToSliceStoreAction>(operatorHandlers.size() - 1);
    }

    auto sliceMergingOperator = std::make_shared<Runtime::Execution::Operators::KeyedSliceMerging>(
        sliceMergingOperatorHandlerIndex, aggregationFunctions, std::move(sliceMergingAction), resultKeyDataTypes, keySize, valueSize);
    pipeline.setRootOperator(sliceMergingOperator);
    return sliceMergingOperator;
}

std::unique_ptr<Runtime::Execution::Operators::TimeFunction>
LowerPhysicalToNautilusOperators::lowerTimeFunction(const Windowing::TimeBasedWindowTypePtr& timeWindow)
{
    /// Depending on the window type we create a different time function.
    /// If the window type is ingestion time or we use the special record creation ts field, create an ingestion time function.
    /// TODO remove record creation ts if it is not needed anymore
    if (timeWindow->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime
        || timeWindow->getTimeCharacteristic()->getField()->getName() == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
    {
        return std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
    }
    else if (timeWindow->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::EventTime)
    {
        /// For event time fields, we look up the reference field name and create an expression to read the field.
        auto timeCharacteristicField = timeWindow->getTimeCharacteristic()->getField()->getName();
        auto timeStampField = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(timeCharacteristicField);
        return std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
            timeStampField, timeWindow->getTimeCharacteristic()->getTimeUnit());
    }
    throw UnknownTimeFunctionType(timeWindow->toString());
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto physicalPreAggregation = NES::Util::as<PhysicalOperators::PhysicalSlicePreAggregationOperator>(physicalOperator);
    if (physicalPreAggregation->getWindowDefinition()->isKeyed())
    {
        return lowerKeyedPreAggregationOperator(pipeline, physicalPreAggregation, operatorHandlers);
    }
    else
    {
        return lowerNonKeyedPreAggregationOperator(pipeline, physicalPreAggregation, operatorHandlers);
    }
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerNonKeyedPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto physicalGTLPAO = NES::Util::as<PhysicalOperators::PhysicalSlicePreAggregationOperator>(physicalOperator);
    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = physicalGTLPAO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    auto timeWindow = NES::Util::as<Windowing::TimeBasedWindowType>(windowDefinition->getWindowType());
    auto timeFunction = lowerTimeFunction(timeWindow);

    auto handler = std::make_shared<Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler>(
        timeWindow->getSize().getTime(), timeWindow->getSlide().getTime(), windowDefinition->getInputOriginIds());

    operatorHandlers.emplace_back(handler);
    auto slicePreAggregation = std::make_shared<Runtime::Execution::Operators::NonKeyedSlicePreAggregation>(
        operatorHandlers.size() - 1, std::move(timeFunction), aggregationFunctions);
    return slicePreAggregation;
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerKeyedPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto physicalGTLPAO = NES::Util::as<PhysicalOperators::PhysicalSlicePreAggregationOperator>(physicalOperator);

    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = windowDefinition->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    auto timeWindow = NES::Util::as<Windowing::TimeBasedWindowType>(windowDefinition->getWindowType());
    auto timeFunction = lowerTimeFunction(timeWindow);
    auto keys = windowDefinition->getKeys();
    PRECONDITION(!keys.empty(), "expected at least one key field for keyed pre-aggregation operator");
    std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyReadExpressions;
    auto df = DefaultPhysicalTypeFactory();
    std::vector<PhysicalTypePtr> keyDataTypes;
    for (const auto& key : keys)
    {
        keyReadExpressions.emplace_back(expressionProvider->lowerExpression(key));
        keyDataTypes.emplace_back(df.getPhysicalType(key->getStamp()));
    }

    auto handler = std::make_shared<Runtime::Execution::Operators::KeyedSlicePreAggregationHandler>(
        timeWindow->getSize().getTime(), timeWindow->getSlide().getTime(), windowDefinition->getInputOriginIds());

    operatorHandlers.emplace_back(handler);
    auto sliceMergingOperator = std::make_shared<Runtime::Execution::Operators::KeyedSlicePreAggregation>(
        operatorHandlers.size() - 1,
        std::move(timeFunction),
        keyReadExpressions,
        keyDataTypes,
        aggregationFunctions,
        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerWatermarkAssignmentOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
    std::vector<Runtime::Execution::OperatorHandlerPtr>&)
{
    auto wao = NES::Util::as<PhysicalOperators::PhysicalWatermarkAssignmentOperator>(operatorPtr);

    ///Add either event time or ingestion time watermark strategy
    if (NES::Util::instanceOf<Windowing::EventTimeWatermarkStrategyDescriptor>(wao->getWatermarkStrategyDescriptor()))
    {
        auto eventTimeWatermarkStrategy
            = NES::Util::as<Windowing::EventTimeWatermarkStrategyDescriptor>(wao->getWatermarkStrategyDescriptor());
        auto fieldExpression = expressionProvider->lowerExpression(eventTimeWatermarkStrategy->getOnField());
        auto watermarkAssignmentOperator = std::make_shared<Runtime::Execution::Operators::EventTimeWatermarkAssignment>(
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(fieldExpression, eventTimeWatermarkStrategy->getTimeUnit()));
        return watermarkAssignmentOperator;
    }
    else if (NES::Util::instanceOf<Windowing::IngestionTimeWatermarkStrategyDescriptor>(wao->getWatermarkStrategyDescriptor()))
    {
        auto watermarkAssignmentOperator = std::make_shared<Runtime::Execution::Operators::IngestionTimeWatermarkAssignment>(
            std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>());
        return watermarkAssignmentOperator;
    }
    else
    {
        throw UnknownWatermarkStrategy();
    }
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerScan(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorNode, size_t bufferSize)
{
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only row layout is supported");
    /// pass buffer size here
    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Runtime::Execution::MemoryProvider::MemoryProvider> memoryProvider
        = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerEmit(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorNode, size_t bufferSize)
{
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only row layout is supported");
    /// pass buffer size here
    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Runtime::Execution::MemoryProvider::MemoryProvider> memoryProvider
        = std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Emit>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerFilter(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorPtr)
{
    auto filterOperator = NES::Util::as<PhysicalOperators::PhysicalFilterOperator>(operatorPtr);
    auto expression = expressionProvider->lowerExpression(filterOperator->getPredicate());
    return std::make_shared<Runtime::Execution::Operators::Selection>(expression);
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerMap(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorPtr)
{
    auto mapOperator = NES::Util::as<PhysicalOperators::PhysicalMapOperator>(operatorPtr);
    auto assignmentField = mapOperator->getMapExpression()->getField();
    auto assignmentExpression = mapOperator->getMapExpression()->getAssignment();
    auto expression = expressionProvider->lowerExpression(assignmentExpression);
    auto writeField = std::make_shared<Runtime::Execution::Expressions::WriteFieldExpression>(assignmentField->getFieldName(), expression);
    return std::make_shared<Runtime::Execution::Operators::Map>(writeField);
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerThresholdWindow(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorPtr, uint64_t handlerIndex)
{
    NES_INFO("lowerThresholdWindow {} and handlerid {}", operatorPtr->toString(), handlerIndex);
    auto thresholdWindowOperator = NES::Util::as<PhysicalOperators::PhysicalThresholdWindowOperator>(operatorPtr);
    auto contentBasedWindowType
        = NES::Util::as<Windowing::ContentBasedWindowType>(thresholdWindowOperator->getWindowDefinition()->getWindowType());
    auto thresholdWindowType = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
    NES_INFO("lowerThresholdWindow Predicate {}", thresholdWindowType->getPredicate()->toString());
    auto predicate = expressionProvider->lowerExpression(thresholdWindowType->getPredicate());
    auto minCount = thresholdWindowType->getMinimumCount();

    auto aggregations = thresholdWindowOperator->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    std::vector<std::string> aggregationResultFieldNames;
    std::transform(
        aggregations.cbegin(),
        aggregations.cend(),
        std::back_inserter(aggregationResultFieldNames),
        [&](const Windowing::WindowAggregationDescriptorPtr& agg)
        { return NES::Util::as_if<FieldAccessExpressionNode>(agg->as())->getFieldName(); });

    return std::make_shared<Runtime::Execution::Operators::NonKeyedThresholdWindow>(
        predicate, aggregationResultFieldNames, minCount, aggregationFunctions, handlerIndex);
}

std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>>
LowerPhysicalToNautilusOperators::lowerAggregations(const std::vector<Windowing::WindowAggregationDescriptorPtr>& aggs)
{
    NES_INFO("Lower Window Aggregations to Nautilus Operator");
    std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>> aggregationFunctions;
    std::transform(
        aggs.cbegin(),
        aggs.cend(),
        std::back_inserter(aggregationFunctions),
        [&](const Windowing::WindowAggregationDescriptorPtr& agg) -> std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>
        {
            DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();

            /// lower the data types
            auto physicalInputType = physicalTypeFactory.getPhysicalType(agg->getInputStamp());
            auto physicalFinalType = physicalTypeFactory.getPhysicalType(agg->getFinalAggregateStamp());

            auto aggregationInputExpression = expressionProvider->lowerExpression(agg->on());
            std::string aggregationResultFieldIdentifier;
            if (auto fieldAccessExpression = NES::Util::as_if<FieldAccessExpressionNode>(agg->as()))
            {
                aggregationResultFieldIdentifier = fieldAccessExpression->getFieldName();
            }
            else
            {
                throw UnknownAggregationType("complex expressions as fields are not supported");
            }
            switch (agg->getType())
            {
                case Windowing::WindowAggregationDescriptor::Type::Avg:
                    return std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(
                        physicalInputType, physicalFinalType, aggregationInputExpression, aggregationResultFieldIdentifier);
                case Windowing::WindowAggregationDescriptor::Type::Count:
                    return std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(
                        physicalInputType, physicalFinalType, aggregationInputExpression, aggregationResultFieldIdentifier);
                case Windowing::WindowAggregationDescriptor::Type::Max:
                    return std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(
                        physicalInputType, physicalFinalType, aggregationInputExpression, aggregationResultFieldIdentifier);
                case Windowing::WindowAggregationDescriptor::Type::Min:
                    return std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(
                        physicalInputType, physicalFinalType, aggregationInputExpression, aggregationResultFieldIdentifier);
                case Windowing::WindowAggregationDescriptor::Type::Median:
                    /// TODO 3331: add median aggregation function
                    break;
                case Windowing::WindowAggregationDescriptor::Type::Sum: {
                    return std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(
                        physicalInputType, physicalFinalType, aggregationInputExpression, aggregationResultFieldIdentifier);
                }
            };
            throw UnknownAggregationType("complex expressions as fields are not supported");
        });
    return aggregationFunctions;
}

std::unique_ptr<Runtime::Execution::Aggregation::AggregationValue> LowerPhysicalToNautilusOperators::getAggregationValueForThresholdWindow(
    Windowing::WindowAggregationDescriptor::Type aggregationType, DataTypePtr inputType)
{
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto physicalType = physicalTypeFactory.getPhysicalType(std::move(inputType));
    auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
    /// TODO 3468: Check if we can make this ugly nested switch case better
    switch (aggregationType)
    {
        case Windowing::WindowAggregationDescriptor::Type::Avg:
            switch (basicType->nativeType)
            {
                case BasicPhysicalType::NativeType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int8_t>>();
                case BasicPhysicalType::NativeType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int16_t>>();
                case BasicPhysicalType::NativeType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int32_t>>();
                case BasicPhysicalType::NativeType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int64_t>>();
                case BasicPhysicalType::NativeType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint8_t>>();
                case BasicPhysicalType::NativeType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint16_t>>();
                case BasicPhysicalType::NativeType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint32_t>>();
                case BasicPhysicalType::NativeType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint64_t>>();
                case BasicPhysicalType::NativeType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<float_t>>();
                case BasicPhysicalType::NativeType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<double_t>>();
                default:
                    throw UnknownAggregationType("Unknown aggregation type for window aggregation avg");
            }
        case Windowing::WindowAggregationDescriptor::Type::Count:
            switch (basicType->nativeType)
            {
                case BasicPhysicalType::NativeType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int8_t>>();
                case BasicPhysicalType::NativeType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int16_t>>();
                case BasicPhysicalType::NativeType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int32_t>>();
                case BasicPhysicalType::NativeType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int64_t>>();
                case BasicPhysicalType::NativeType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint8_t>>();
                case BasicPhysicalType::NativeType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint16_t>>();
                case BasicPhysicalType::NativeType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint32_t>>();
                case BasicPhysicalType::NativeType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint64_t>>();
                case BasicPhysicalType::NativeType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<float_t>>();
                case BasicPhysicalType::NativeType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<double_t>>();
                default:
                    throw UnknownAggregationType("Unknown aggregation type for window aggregation count");
            }
        case Windowing::WindowAggregationDescriptor::Type::Max:
            switch (basicType->nativeType)
            {
                case BasicPhysicalType::NativeType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int8_t>>();
                case BasicPhysicalType::NativeType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int16_t>>();
                case BasicPhysicalType::NativeType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int32_t>>();
                case BasicPhysicalType::NativeType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int64_t>>();
                case BasicPhysicalType::NativeType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint8_t>>();
                case BasicPhysicalType::NativeType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint16_t>>();
                case BasicPhysicalType::NativeType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint32_t>>();
                case BasicPhysicalType::NativeType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint64_t>>();
                case BasicPhysicalType::NativeType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<float_t>>();
                case BasicPhysicalType::NativeType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<double_t>>();
                default:
                    throw UnknownAggregationType("Unknown aggregation type for window aggregation max");
            }
        case Windowing::WindowAggregationDescriptor::Type::Min:
            switch (basicType->nativeType)
            {
                case BasicPhysicalType::NativeType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int8_t>>();
                case BasicPhysicalType::NativeType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int16_t>>();
                case BasicPhysicalType::NativeType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int32_t>>();
                case BasicPhysicalType::NativeType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int64_t>>();
                case BasicPhysicalType::NativeType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint8_t>>();
                case BasicPhysicalType::NativeType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint16_t>>();
                case BasicPhysicalType::NativeType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint32_t>>();
                case BasicPhysicalType::NativeType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint64_t>>();
                case BasicPhysicalType::NativeType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<float_t>>();
                case BasicPhysicalType::NativeType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<double_t>>();
                default:
                    throw UnknownAggregationType("Unknown aggregation type for window aggregation min");
            }
        case Windowing::WindowAggregationDescriptor::Type::Sum:
            switch (basicType->nativeType)
            {
                case BasicPhysicalType::NativeType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int8_t>>();
                case BasicPhysicalType::NativeType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int16_t>>();
                case BasicPhysicalType::NativeType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int32_t>>();
                case BasicPhysicalType::NativeType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int64_t>>();
                case BasicPhysicalType::NativeType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint8_t>>();
                case BasicPhysicalType::NativeType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint16_t>>();
                case BasicPhysicalType::NativeType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint32_t>>();
                case BasicPhysicalType::NativeType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint64_t>>();
                case BasicPhysicalType::NativeType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<float_t>>();
                case BasicPhysicalType::NativeType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<double_t>>();
                default:
                    throw UnknownAggregationType("Unknown aggregation type for window aggregation sum");
            }
        default:
            throw UnknownAggregationType("Unknown window aggregation type");
    }
}

LowerPhysicalToNautilusOperators::~LowerPhysicalToNautilusOperators() = default;

}
