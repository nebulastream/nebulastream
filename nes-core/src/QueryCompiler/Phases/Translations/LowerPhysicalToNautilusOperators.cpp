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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Execution/Aggregation/CountAggregation.hpp>
#include <Execution/Aggregation/MaxAggregation.hpp>
#include <Execution/Aggregation/MinAggregation.hpp>
#include <Execution/Aggregation/SumAggregation.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/MapJavaUdf.hpp>
#include <Execution/Operators/Relational/MapJavaUdfOperatorHandler.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/GlobalTimeWindow/GlobalThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/Join/JoinPhases/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/JoinPhases/StreamJoinSink.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapJavaUdfOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Phases/Translations/ExpressionProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSliceMergingOperatorHandler.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowMeasures/TimeUnit.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>

#include <utility>

namespace NES::QueryCompilation {

std::shared_ptr<LowerPhysicalToNautilusOperators> LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators::create() {
    return std::make_shared<LowerPhysicalToNautilusOperators>();
}

LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators()
    : expressionProvider(std::make_unique<ExpressionProvider>()) {}

PipelineQueryPlanPtr LowerPhysicalToNautilusOperators::apply(PipelineQueryPlanPtr pipelinedQueryPlan,
                                                             const Runtime::NodeEnginePtr& nodeEngine) {
    auto bufferSize = nodeEngine->getQueryManager()->getBufferManager()->getBufferSize();
    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline, bufferSize);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr LowerPhysicalToNautilusOperators::apply(OperatorPipelinePtr operatorPipeline, size_t bufferSize) {
    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();
    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;
    std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator;

    for (const auto& node : nodes) {
        NES_INFO("Node: " << node->toString());
        parentOperator =
            lower(*pipeline, parentOperator, node->as<PhysicalOperators::PhysicalOperator>(), bufferSize, operatorHandlers);
    }
    for (auto& root : queryPlan->getRootOperators()) {
        queryPlan->removeAsRootOperator(root);
    }
    auto nautilusPipelineWrapper = NautilusPipelineOperator::create(pipeline, operatorHandlers);
    queryPlan->addRootOperator(nautilusPipelineWrapper);
    return operatorPipeline;
}
std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lower(Runtime::Execution::PhysicalOperatorPipeline& pipeline,
                                        std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator,
                                        const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
                                        size_t bufferSize,
                                        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    NES_INFO("Lower node:" << operatorNode->toString() << "to NautilusOperator." );
    if (operatorNode->instanceOf<PhysicalOperators::PhysicalScanOperator>()) {
        auto scan = lowerScan(pipeline, operatorNode, bufferSize);
        pipeline.setRootOperator(scan);
        return scan;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalEmitOperator>()) {
        auto emit = lowerEmit(pipeline, operatorNode, bufferSize);
        parentOperator->setChild(emit);
        return emit;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalFilterOperator>()) {
        auto filter = lowerFilter(pipeline, operatorNode);
        parentOperator->setChild(filter);
        return filter;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalMapOperator>()) {
        auto map = lowerMap(pipeline, operatorNode);
        parentOperator->setChild(map);
        return map;
#ifdef ENABLE_JNI
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalMapJavaUdfOperator>()) {
        auto mapOperator = operatorNode->as<PhysicalOperators::PhysicalMapJavaUdfOperator>();
        // We can't copy the descriptor because it is in nes-core and the MapJavaUdfOperatorHandler is in nes-runtime
        // Thus, to resolve a circular dependency, we need this workaround by coping descriptor elements
        auto mapJavaUdfDescriptor = mapOperator->getJavaUdfDescriptor();
        auto className = mapJavaUdfDescriptor->getClassName();
        auto methodName = mapJavaUdfDescriptor->getMethodName();
        auto byteCodeList = mapJavaUdfDescriptor->getByteCodeList();
        auto inputClassName = mapJavaUdfDescriptor->getInputClassName();
        auto outputClassName = mapJavaUdfDescriptor->getOutputClassName();
        auto inputSchema = mapJavaUdfDescriptor->getInputSchema();
        auto outputSchema = mapJavaUdfDescriptor->getOutputSchema();
        auto serializedInstance = mapJavaUdfDescriptor->getSerializedInstance();
        auto returnType = mapJavaUdfDescriptor->getReturnType();

        auto handler = std::make_shared<Runtime::Execution::Operators::MapJavaUdfOperatorHandler>(className,
                                                                                                  methodName,
                                                                                                  inputClassName,
                                                                                                  outputClassName,
                                                                                                  byteCodeList,
                                                                                                  serializedInstance,
                                                                                                  inputSchema,
                                                                                                  outputSchema,
                                                                                                  std::nullopt);
        operatorHandlers.push_back(handler);
        auto indexForThisHandler = operatorHandlers.size() - 1;

        auto mapJavaUdf = lowerMapJavaUdf(pipeline, operatorNode, indexForThisHandler);
        parentOperator->setChild(mapJavaUdf);
        return mapJavaUdf;
#endif// ENABLE_JNI
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalThresholdWindowOperator>()) {
        auto aggs = operatorNode->as<PhysicalOperators::PhysicalThresholdWindowOperator>()
                        ->getOperatorHandler()
                        ->getWindowDefinition()
                        ->getWindowAggregation();

        std::vector<std::unique_ptr<Runtime::Execution::Aggregation::AggregationValue>> aggValues;
        // iterate over all aggregation functions
        for (size_t i = 0; i < aggs.size(); ++i) {
            auto aggregationType = aggs[i]->getType();
            // collect aggValues for each aggType
            aggValues.emplace_back(getAggregationValueForThresholdWindow(aggregationType, aggs[i]->getInputStamp()));
        }
        // pass aggValues to ThresholdWindowHandler
        auto handler = std::make_shared<Runtime::Execution::Operators::ThresholdWindowOperatorHandler>(std::move(aggValues));
        operatorHandlers.push_back(handler);
        auto indexForThisHandler = operatorHandlers.size() - 1;

        auto thresholdWindow = lowerThresholdWindow(pipeline, operatorNode, indexForThisHandler);
        parentOperator->setChild(thresholdWindow);
        return thresholdWindow;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalThreadLocalPreAggregationOperator>()) {
        auto preAggregationOperator = lowerGlobalThreadLocalPreAggregationOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(preAggregationOperator);
        return preAggregationOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator>()) {
        auto preAggregationOperator = lowerKeyedThreadLocalPreAggregationOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(preAggregationOperator);
        return preAggregationOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalSliceMergingOperator>()) {
        return lowerGlobalSliceMergingOperator(pipeline, operatorNode, operatorHandlers);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedSliceMergingOperator>()) {
        return lowerKeyedSliceMergingOperator(pipeline, operatorNode, operatorHandlers);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalTumblingWindowSink>()) {
        // As the sink is already part of the slice merging, we can ignore this operator for now.
        return parentOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedTumblingWindowSink>()) {
        //  As the sink is already part of the slice merging,  we can ignore this operator for now.
        return parentOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>()) {
        auto watermarkOperator = lowerWatermarkAssignmentOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(watermarkOperator);
        return watermarkOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalProjectOperator>()) {
        // As the projection is part of the emit, we can ignore this operator for now.
        return parentOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalStreamJoinSinkOperator>()) {
        auto sinkOperator = operatorNode->as<PhysicalOperators::PhysicalStreamJoinSinkOperator>();

        NES_DEBUG("Added streamJoinOpHandler to operatorHandlers!");
        operatorHandlers.push_back(sinkOperator->getOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto joinSinkNautilus = std::make_shared<Runtime::Execution::Operators::StreamJoinSink>(handlerIndex);
        pipeline.setRootOperator(joinSinkNautilus);
        return joinSinkNautilus;

    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalStreamJoinBuildOperator>()) {
        auto buildOperator = operatorNode->as<PhysicalOperators::PhysicalStreamJoinBuildOperator>();

        NES_DEBUG("Added streamJoinOpHandler to operatorHandlers!");
        operatorHandlers.push_back(buildOperator->getOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto isLeftSide = buildOperator->getBuildSide() == JoinBuildSideType::Left;
        auto joinFieldName = isLeftSide ? buildOperator->getOperatorHandler()->getJoinFieldNameLeft()
                                        : buildOperator->getOperatorHandler()->getJoinFieldNameRight();
        auto joinSchema = isLeftSide ? buildOperator->getOperatorHandler()->getJoinSchemaLeft()
                                     : buildOperator->getOperatorHandler()->getJoinSchemaRight();

        auto joinBuildNautilus =
            std::make_shared<Runtime::Execution::Operators::StreamJoinBuild>(handlerIndex,
                                                                             isLeftSide,
                                                                             joinFieldName,
                                                                             buildOperator->getTimeStampFieldName(),
                                                                             joinSchema);

        parentOperator->setChild(std::dynamic_pointer_cast<Runtime::Execution::Operators::ExecutableOperator>(joinBuildNautilus));
        return joinBuildNautilus;
    }
    NES_NOT_IMPLEMENTED();
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerGlobalSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGSMO = physicalOperator->as<PhysicalOperators::PhysicalGlobalSliceMergingOperator>();
    auto handler =
        std::get<std::shared_ptr<Runtime::Execution::Operators::GlobalSliceMergingHandler>>(physicalGSMO->getWindowHandler());
    operatorHandlers.emplace_back(handler);
    auto aggregations = physicalGSMO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    std::vector<std::string> aggregationFields;
    std::transform(aggregations.cbegin(),
                   aggregations.cend(),
                   std::back_inserter(aggregationFields),
                   [&](const Windowing::WindowAggregationDescriptorPtr& agg) {
                       return agg->as()->as_if<FieldAccessExpressionNode>()->getFieldName();
                   });
    // We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    // TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalGSMO->getOutputSchema()->get(0)->getName();
    auto endTs = physicalGSMO->getOutputSchema()->get(1)->getName();
    auto sliceMergingOperator =
        std::make_shared<Runtime::Execution::Operators::GlobalSliceMerging>(operatorHandlers.size() - 1,
                                                                            aggregationFunctions,
                                                                            aggregationFields,
                                                                            startTs,
                                                                            endTs,
                                                                            physicalGSMO->getWindowDefinition()->getOriginId());
    pipeline.setRootOperator(sliceMergingOperator);
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerKeyedSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGSMO = physicalOperator->as<PhysicalOperators::PhysicalKeyedSliceMergingOperator>();
    auto handler =
        std::get<std::shared_ptr<Runtime::Execution::Operators::KeyedSliceMergingHandler>>(physicalGSMO->getWindowHandler());
    operatorHandlers.emplace_back(handler);
    auto aggregations = physicalGSMO->getWindowDefinition()->getWindowAggregation();
    std::vector<std::string> resultAggregationFields;
    std::transform(aggregations.cbegin(),
                   aggregations.cend(),
                   std::back_inserter(resultAggregationFields),
                   [&](const Windowing::WindowAggregationDescriptorPtr& agg) {
                       return agg->as()->as_if<FieldAccessExpressionNode>()->getFieldName();
                   });
    auto aggregationFunctions = lowerAggregations(aggregations);
    // We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    // TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalGSMO->getOutputSchema()->get(0)->getName();
    auto endTs = physicalGSMO->getOutputSchema()->get(1)->getName();
    auto keys = physicalGSMO->getWindowDefinition()->getKeys();

    std::vector<std::string> resultKeyFields;
    std::vector<PhysicalTypePtr> keyDataTypes;
    for (const auto& key : keys) {
        resultKeyFields.emplace_back(key->getFieldName());
        keyDataTypes.emplace_back(DefaultPhysicalTypeFactory().getPhysicalType(key->getStamp()));
    }
    auto sliceMergingOperator =
        std::make_shared<Runtime::Execution::Operators::KeyedSliceMerging>(operatorHandlers.size() - 1,
                                                                           aggregationFunctions,
                                                                           resultAggregationFields,
                                                                           keyDataTypes,
                                                                           resultKeyFields,
                                                                           startTs,
                                                                           endTs,
                                                                           physicalGSMO->getWindowDefinition()->getOriginId());
    pipeline.setRootOperator(sliceMergingOperator);
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerGlobalThreadLocalPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGTLPAO = physicalOperator->as<PhysicalOperators::PhysicalGlobalThreadLocalPreAggregationOperator>();
    auto handler = std::get<std::shared_ptr<Runtime::Execution::Operators::GlobalSlicePreAggregationHandler>>(
        physicalGTLPAO->getWindowHandler());
    operatorHandlers.emplace_back(handler);
    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = physicalGTLPAO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);

    std::vector<Runtime::Execution::Expressions::ExpressionPtr> aggregationFields;
    std::transform(aggregations.cbegin(), aggregations.cend(), std::back_inserter(aggregationFields), [&](auto& agg) {
        return expressionProvider->lowerExpression(agg->on());
    });
    auto timeWindow = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto timeCharacteristicField = timeWindow->getTimeCharacteristic()->getField()->getName();
    auto timeStampField = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(timeCharacteristicField);
    auto sliceMergingOperator =
        std::make_shared<Runtime::Execution::Operators::GlobalSlicePreAggregation>(operatorHandlers.size() - 1,
                                                                                   timeStampField,
                                                                                   aggregationFields,
                                                                                   aggregationFunctions);
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerKeyedThreadLocalPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGTLPAO = physicalOperator->as<PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator>();
    auto handler = std::get<std::shared_ptr<Runtime::Execution::Operators::KeyedSlicePreAggregationHandler>>(
        physicalGTLPAO->getWindowHandler());
    operatorHandlers.emplace_back(handler);
    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = windowDefinition->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);

    std::vector<Runtime::Execution::Expressions::ExpressionPtr> aggregationFields;
    std::transform(aggregations.cbegin(), aggregations.cend(), std::back_inserter(aggregationFields), [&](auto& agg) {
        return expressionProvider->lowerExpression(agg->on());
    });
    auto timeWindow = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto timeCharacteristicField = timeWindow->getTimeCharacteristic()->getField()->getName();
    auto timeStampField = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(timeCharacteristicField);

    auto keys = windowDefinition->getKeys();
    NES_ASSERT(!keys.empty(), "A keyed window should have keys");
    std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyReadExpressions;
    auto df = DefaultPhysicalTypeFactory();
    std::vector<PhysicalTypePtr> keyDataTypes;
    for (const auto& key : keys) {
        keyReadExpressions.emplace_back(expressionProvider->lowerExpression(key));
        keyDataTypes.emplace_back(df.getPhysicalType(key->getStamp()));
    }

    auto keyExpressions = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(timeCharacteristicField);
    auto sliceMergingOperator = std::make_shared<Runtime::Execution::Operators::KeyedSlicePreAggregation>(
        operatorHandlers.size() - 1,
        timeStampField,
        keyReadExpressions,
        keyDataTypes,
        aggregationFields,
        aggregationFunctions,
        std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerWatermarkAssignmentOperator(Runtime::Execution::PhysicalOperatorPipeline&,
                                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
                                                                   std::vector<Runtime::Execution::OperatorHandlerPtr>&) {
    auto wao = operatorPtr->as<PhysicalOperators::PhysicalWatermarkAssignmentOperator>();
    auto eventTimeWatermarkStrategy =
        wao->getWatermarkStrategyDescriptor()->as<Windowing::EventTimeWatermarkStrategyDescriptor>();
    auto fieldExpression = expressionProvider->lowerExpression(eventTimeWatermarkStrategy->getOnField());
    auto watermarkAssignmentOperator =
        std::make_shared<Runtime::Execution::Operators::EventTimeWatermarkAssignment>(fieldExpression);
    return watermarkAssignmentOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lowerScan(Runtime::Execution::PhysicalOperatorPipeline&,
                                            const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
                                            size_t bufferSize) {
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::ROW_LAYOUT, "Currently only row layout is supported");
    // pass buffer size here
    auto layout = std::make_shared<Runtime::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Runtime::Execution::MemoryProvider::MemoryProvider> memoryProvider =
        std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerEmit(Runtime::Execution::PhysicalOperatorPipeline&,
                                            const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
                                            size_t bufferSize) {
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::ROW_LAYOUT, "Currently only row layout is supported");
    // pass buffer size here
    auto layout = std::make_shared<Runtime::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Runtime::Execution::MemoryProvider::MemoryProvider> memoryProvider =
        std::make_unique<Runtime::Execution::MemoryProvider::RowMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Emit>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerFilter(Runtime::Execution::PhysicalOperatorPipeline&,
                                              const PhysicalOperators::PhysicalOperatorPtr& operatorPtr) {
    auto filterOperator = operatorPtr->as<PhysicalOperators::PhysicalFilterOperator>();
    auto expression = expressionProvider->lowerExpression(filterOperator->getPredicate());
    return std::make_shared<Runtime::Execution::Operators::Selection>(expression);
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerMap(Runtime::Execution::PhysicalOperatorPipeline&,
                                           const PhysicalOperators::PhysicalOperatorPtr& operatorPtr) {
    auto mapOperator = operatorPtr->as<PhysicalOperators::PhysicalMapOperator>();
    auto assignmentField = mapOperator->getMapExpression()->getField();
    auto assignmentExpression = mapOperator->getMapExpression()->getAssignment();
    auto expression = expressionProvider->lowerExpression(assignmentExpression);
    auto writeField =
        std::make_shared<Runtime::Execution::Expressions::WriteFieldExpression>(assignmentField->getFieldName(), expression);
    return std::make_shared<Runtime::Execution::Operators::Map>(writeField);
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerThresholdWindow(Runtime::Execution::PhysicalOperatorPipeline&,
                                                       const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
                                                       uint64_t handlerIndex) {
    NES_INFO("lowerThresholdWindow " << operatorPtr->toString() << "and handlerid " << handlerIndex);
    auto thresholdWindowOperator = operatorPtr->as<PhysicalOperators::PhysicalThresholdWindowOperator>();
    auto contentBasedWindowType = Windowing::ContentBasedWindowType::asContentBasedWindowType(
        thresholdWindowOperator->getOperatorHandler()->getWindowDefinition()->getWindowType());
    auto thresholdWindowType = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
    NES_INFO("lowerThresholdWindow Predicate" << thresholdWindowType->getPredicate());
    auto predicate = expressionProvider->lowerExpression(thresholdWindowType->getPredicate());
    auto minCount = thresholdWindowType->getMinimumCount();

    auto aggregations = thresholdWindowOperator->getOperatorHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<Runtime::Execution::Aggregation::AggregationFunctionPtr> aggregationFunctions;
    std::vector<std::string> aggregationResultFieldNames;
    std::vector<std::shared_ptr<Runtime::Execution::Expressions::Expression>> aggregatedFieldAccesses;
    // iterate over all aggregation function and lower them
    for (int64_t i = 0; i < (int64_t) aggregations.size(); ++i) {
        auto aggregation = aggregations[i];// get the aggregation function
        NES_INFO("lowerThresholdWindow Aggregations: " << aggregation->toString());
        auto aggregationFunction = lowerAggregations(aggregations)[i];
        aggregationFunctions.emplace_back(aggregationFunction);
        // Obtain the field name used to store the aggregation result
        auto thresholdWindowResultSchema =
            operatorPtr->as<PhysicalOperators::PhysicalThresholdWindowOperator>()->getOperatorHandler()->getResultSchema();
        auto aggregationResultFieldName =
            //TODO: we get an error here : Schema::getQualifierNameForSystemGeneratedFields: a schema is not allowed to be empty when a qualifier is requested
            //because our result schema is empty, I excluded threshold window from the output schema creation in the GlobalWindowOperator as
            // this caused another error: attr does not exit (for agg fields)
            //Thus, here we now get only Avg, Min, etc. as resultfieldname but that does not affact things further so far.
            thresholdWindowResultSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + aggregation->getTypeAsString();
        aggregationResultFieldNames.emplace_back(aggregationResultFieldName);
        /** check if the aggregation is not of type Count
          *  count is treated different as does not agg onField but onTuple thus the onField for count should not be set to anything
          *  and we want that all vectors, i.e., aggregationFunctions, aggregationResultFieldNames, and aggregatedFieldAccesses
          *  have corresponding indexes. Thus, we add a null pointer as access field for count agg.
          */
        if (aggregation->getType() != Windowing::WindowAggregationDescriptor::Count) {
            auto aggregatedFieldAccess = expressionProvider->lowerExpression(aggregation->on());
            aggregatedFieldAccesses.emplace_back(aggregatedFieldAccess);
        } else {
            auto aggregatedFieldAccess = nullptr;
            aggregatedFieldAccesses.emplace_back(aggregatedFieldAccess);
        }
    }

    return std::make_shared<Runtime::Execution::Operators::ThresholdWindow>(predicate,
                                                                            minCount,
                                                                            aggregatedFieldAccesses,
                                                                            aggregationResultFieldNames,
                                                                            aggregationFunctions,
                                                                            handlerIndex);
}

#ifdef ENABLE_JNI
std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerMapJavaUdf(Runtime::Execution::PhysicalOperatorPipeline&,
                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
                                                  uint64_t handlerIndex) {
    auto mapOperator = operatorPtr->as<PhysicalOperators::PhysicalMapJavaUdfOperator>();
    auto mapJavaUdfDescriptor = mapOperator->getJavaUdfDescriptor();
    auto inputSchema = mapJavaUdfDescriptor->getInputSchema();
    auto outputSchema = mapJavaUdfDescriptor->getOutputSchema();

    return std::make_shared<Runtime::Execution::Operators::MapJavaUdf>(handlerIndex, inputSchema, outputSchema);
}
#endif// ENABLE_JNI

std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>>
LowerPhysicalToNautilusOperators::lowerAggregations(const std::vector<Windowing::WindowAggregationPtr>& aggs) {
    NES_INFO("Lower Window Aggregations to Nautilus Operator");
    std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>> aggregationFunctions;
    std::transform(
        aggs.cbegin(),
        aggs.cend(),
        std::back_inserter(aggregationFunctions),
        [&](const auto& agg) -> std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction> {
            DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();

            // lower the data types
            auto physicalInputType = physicalTypeFactory.getPhysicalType(agg->getInputStamp());
            auto physicalFinalType = physicalTypeFactory.getPhysicalType(agg->getFinalAggregateStamp());

            switch (agg->getType()) {
                case Windowing::WindowAggregationDescriptor::Avg:
                    return std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(physicalInputType,
                                                                                                     physicalFinalType);
                case Windowing::WindowAggregationDescriptor::Count:
                    return std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(physicalInputType,
                                                                                                       physicalFinalType);
                case Windowing::WindowAggregationDescriptor::Max:
                    return std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(physicalInputType,
                                                                                                     physicalFinalType);
                case Windowing::WindowAggregationDescriptor::Min:
                    return std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(physicalInputType,
                                                                                                     physicalFinalType);
                case Windowing::WindowAggregationDescriptor::Median:
                    // TODO 3331: add median aggregation function
                    break;
                case Windowing::WindowAggregationDescriptor::Sum: {
                    return std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(physicalInputType,
                                                                                                     physicalFinalType);
                }
            };
            NES_NOT_IMPLEMENTED();
        });
    return aggregationFunctions;
}
std::unique_ptr<Runtime::Execution::Aggregation::AggregationValue>
LowerPhysicalToNautilusOperators::getAggregationValueForThresholdWindow(
    Windowing::WindowAggregationDescriptor::Type aggregationType,
    DataTypePtr inputType) {
    DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();
    auto physicalType = physicalTypeFactory.getPhysicalType(std::move(inputType));
    auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
    // TODO 3468: Check if we can make this ugly nested switch case better
    switch (aggregationType) {
        case Windowing::WindowAggregationDescriptor::Avg:
            switch (basicType->nativeType) {
                case BasicPhysicalType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int8_t>>();
                case BasicPhysicalType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int16_t>>();
                case BasicPhysicalType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int32_t>>();
                case BasicPhysicalType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<int64_t>>();
                case BasicPhysicalType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint8_t>>();
                case BasicPhysicalType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint16_t>>();
                case BasicPhysicalType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint32_t>>();
                case BasicPhysicalType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<uint64_t>>();
                case BasicPhysicalType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<float_t>>();
                case BasicPhysicalType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::AvgAggregationValue<double_t>>();
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        case Windowing::WindowAggregationDescriptor::Count:
            switch (basicType->nativeType) {
                case BasicPhysicalType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int8_t>>();
                case BasicPhysicalType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int16_t>>();
                case BasicPhysicalType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int32_t>>();
                case BasicPhysicalType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<int64_t>>();
                case BasicPhysicalType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint8_t>>();
                case BasicPhysicalType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint16_t>>();
                case BasicPhysicalType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint32_t>>();
                case BasicPhysicalType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<uint64_t>>();
                case BasicPhysicalType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<float_t>>();
                case BasicPhysicalType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::CountAggregationValue<double_t>>();
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        case Windowing::WindowAggregationDescriptor::Max:
            switch (basicType->nativeType) {
                case BasicPhysicalType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int8_t>>();
                case BasicPhysicalType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int16_t>>();
                case BasicPhysicalType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int32_t>>();
                case BasicPhysicalType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<int64_t>>();
                case BasicPhysicalType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint8_t>>();
                case BasicPhysicalType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint16_t>>();
                case BasicPhysicalType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint32_t>>();
                case BasicPhysicalType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<uint64_t>>();
                case BasicPhysicalType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<float_t>>();
                case BasicPhysicalType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::MaxAggregationValue<double_t>>();
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        case Windowing::WindowAggregationDescriptor::Min:
            switch (basicType->nativeType) {
                case BasicPhysicalType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int8_t>>();
                case BasicPhysicalType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int16_t>>();
                case BasicPhysicalType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int32_t>>();
                case BasicPhysicalType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<int64_t>>();
                case BasicPhysicalType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint8_t>>();
                case BasicPhysicalType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint16_t>>();
                case BasicPhysicalType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint32_t>>();
                case BasicPhysicalType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<uint64_t>>();
                case BasicPhysicalType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<float_t>>();
                case BasicPhysicalType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::MinAggregationValue<double_t>>();
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        case Windowing::WindowAggregationDescriptor::Sum:
            switch (basicType->nativeType) {
                case BasicPhysicalType::INT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int8_t>>();
                case BasicPhysicalType::INT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int16_t>>();
                case BasicPhysicalType::INT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int32_t>>();
                case BasicPhysicalType::INT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<int64_t>>();
                case BasicPhysicalType::UINT_8:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint8_t>>();
                case BasicPhysicalType::UINT_16:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint16_t>>();
                case BasicPhysicalType::UINT_32:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint32_t>>();
                case BasicPhysicalType::UINT_64:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<uint64_t>>();
                case BasicPhysicalType::FLOAT:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<float_t>>();
                case BasicPhysicalType::DOUBLE:
                    return std::make_unique<Runtime::Execution::Aggregation::SumAggregationValue<double_t>>();
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        default: NES_THROW_RUNTIME_ERROR("Unsupported aggregation type");
    }
}

LowerPhysicalToNautilusOperators::~LowerPhysicalToNautilusOperators() = default;

}// namespace NES::QueryCompilation