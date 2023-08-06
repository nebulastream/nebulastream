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
#include <API/Expressions/Expressions.hpp>
#include <API/Schema.hpp>
#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
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
#include <Execution/Operators/Relational/JavaUDF/FlatMapJavaUDF.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
#include <Execution/Operators/Relational/Limit.hpp>
#include <Execution/Operators/Relational/Map.hpp>
#include <Execution/Operators/Relational/Project.hpp>
#include <Execution/Operators/Relational/PythonUDF/MapPythonUDF.hpp>
#include <Execution/Operators/Relational/PythonUDF/PythonUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/Selection.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketPreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/NonKeyedBucketPreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/NonKeyedBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/InferModel/InferModelHandler.hpp>
#include <Execution/Operators/Streaming/InferModel/InferModelOperator.hpp>
#include <Execution/Operators/Streaming/IngestionTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/JoinPhases/StreamHashJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/JoinPhases/StreamHashJoinProbe.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindowOperatorHandler.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalHashJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalNestedLoopJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFlatMapUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalLimitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapUDFOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedGlobalSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/NonKeyedTimeWindow/PhysicalNonKeyedWindowSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Phases/Translations/ExpressionProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedSliceMergingOperatorHandler.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowMeasures/TimeUnit.hpp>
#include <Windowing/WindowTypes/ContentBasedWindowType.hpp>
#include <Windowing/WindowTypes/ThresholdWindow.hpp>
#include <cstddef>
#include <string_view>
#include <utility>

namespace NES::QueryCompilation {

std::shared_ptr<LowerPhysicalToNautilusOperators> LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators::create(
    const QueryCompilation::QueryCompilerOptionsPtr& options) {
    return std::make_shared<LowerPhysicalToNautilusOperators>(options);
}

LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators(const QueryCompilation::QueryCompilerOptionsPtr& options)
    : options(options), expressionProvider(std::make_unique<ExpressionProvider>()) {}

PipelineQueryPlanPtr LowerPhysicalToNautilusOperators::apply(PipelineQueryPlanPtr pipelinedQueryPlan, size_t bufferSize) {
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
        NES_INFO("Node: {}", node->toString());
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
    NES_INFO("Lower node:{} to NautilusOperator.", operatorNode->toString());
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
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalLimitOperator>()) {
        auto limit = lowerLimit(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(limit);
        return limit;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalMapOperator>()) {
        auto map = lowerMap(pipeline, operatorNode);
        parentOperator->setChild(map);
        return map;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalMapUDFOperator>()) {
        // for creating the handler that the nautilus udf operator needs to execute the udf
        const auto udfOperator = operatorNode->as<PhysicalOperators::PhysicalMapUDFOperator>();
        const auto udfDescriptor = udfOperator->getUDFDescriptor();
        const auto methodName = udfDescriptor->getMethodName();
        const auto udfInputSchema = udfDescriptor->getInputSchema();
        const auto udfOutputSchema = udfDescriptor->getOutputSchema();

        // for converting the Physical UDF Operator to the Nautilus Operator
        const auto operatorInputSchema = udfOperator->getInputSchema();
        const auto operatorOutputSchema = udfOperator->getOutputSchema();

        if (udfDescriptor->instanceOf<Catalogs::UDF::JavaUDFDescriptor>()) {
#ifdef ENABLE_JNI
            // creating the java udf handler
            const auto javaUDFDescriptor = udfDescriptor->as<Catalogs::UDF::JavaUDFDescriptor>(udfDescriptor);
            const auto className = javaUDFDescriptor->getClassName();
            const auto byteCodeList = javaUDFDescriptor->getByteCodeList();
            const auto inputClassName = javaUDFDescriptor->getInputClassName();
            const auto outputClassName = javaUDFDescriptor->getOutputClassName();
            const auto serializedInstance = javaUDFDescriptor->getSerializedInstance();
            const auto returnType = javaUDFDescriptor->getReturnType();

            const auto handler = std::make_shared<Runtime::Execution::Operators::JavaUDFOperatorHandler>(className,
                                                                                                         methodName,
                                                                                                         inputClassName,
                                                                                                         outputClassName,
                                                                                                         byteCodeList,
                                                                                                         serializedInstance,
                                                                                                         udfInputSchema,
                                                                                                         udfOutputSchema,
                                                                                                         std::nullopt);

            operatorHandlers.push_back(handler);
            const auto indexForThisHandler = operatorHandlers.size() - 1;

            auto mapJavaUDF = std::make_shared<Runtime::Execution::Operators::MapJavaUDF>(indexForThisHandler,
                                                                                          operatorInputSchema,
                                                                                          operatorOutputSchema);
            parentOperator->setChild(mapJavaUDF);
            return mapJavaUDF;
#endif// ENABLE_JNI
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
        } else if (udfDescriptor->instanceOf<Catalogs::UDF::PythonUDFDescriptor>()) {
            // creating the python udf handler
            const auto pythonUDFDescriptor = udfDescriptor->as<Catalogs::UDF::PythonUDFDescriptor>(udfDescriptor);
            const auto functionString = pythonUDFDescriptor->getFunctionString();

            const auto handler = std::make_shared<Runtime::Execution::Operators::PythonUDFOperatorHandler>(functionString,
                                                                                                           methodName,
                                                                                                           udfInputSchema,
                                                                                                           udfOutputSchema);
            operatorHandlers.push_back(handler);
            const auto indexForThisHandler = operatorHandlers.size() - 1;

            // auto mapPythonUDF = lowerMapPythonUDF(pipeline, operatorNode, indexForThisHandler);
            auto mapPythonUDF = std::make_shared<Runtime::Execution::Operators::MapPythonUDF>(indexForThisHandler,
                                                                                              operatorInputSchema,
                                                                                              operatorOutputSchema);
            parentOperator->setChild(mapPythonUDF);
            return mapPythonUDF;
#endif// NAUTILUS_PYTHON_UDF_ENABLED
        } else {
            NES_NOT_IMPLEMENTED();
        }
#ifdef ENABLE_JNI
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalFlatMapUDFOperator>()) {
        // for creating the handler that the nautilus udf operator needs to execute the udf
        const auto udfOperator = operatorNode->as<PhysicalOperators::PhysicalFlatMapUDFOperator>();
        const auto udfDescriptor = udfOperator->getUDFDescriptor();
        const auto methodName = udfDescriptor->getMethodName();
        const auto udfInputSchema = udfDescriptor->getInputSchema();
        const auto udfOutputSchema = udfDescriptor->getOutputSchema();

        // for converting the Physical UDF Operator to the Nautilus Operator
        const auto operatorInputSchema = udfOperator->getInputSchema();
        const auto operatorOutputSchema = udfOperator->getOutputSchema();

        if (udfDescriptor->instanceOf<Catalogs::UDF::JavaUDFDescriptor>()) {

            // creating the java udf handler
            const auto javaUDFDescriptor = udfDescriptor->as<Catalogs::UDF::JavaUDFDescriptor>(udfDescriptor);
            const auto className = javaUDFDescriptor->getClassName();
            const auto byteCodeList = javaUDFDescriptor->getByteCodeList();
            const auto inputClassName = javaUDFDescriptor->getInputClassName();
            const auto outputClassName = javaUDFDescriptor->getOutputClassName();
            const auto serializedInstance = javaUDFDescriptor->getSerializedInstance();
            const auto returnType = javaUDFDescriptor->getReturnType();

            const auto handler = std::make_shared<Runtime::Execution::Operators::JavaUDFOperatorHandler>(className,
                                                                                                         methodName,
                                                                                                         inputClassName,
                                                                                                         outputClassName,
                                                                                                         byteCodeList,
                                                                                                         serializedInstance,
                                                                                                         udfInputSchema,
                                                                                                         udfOutputSchema,
                                                                                                         std::nullopt);

            operatorHandlers.push_back(handler);
            const auto indexForThisHandler = operatorHandlers.size() - 1;
            auto flatMapJavaUDF = std::make_shared<Runtime::Execution::Operators::FlatMapJavaUDF>(indexForThisHandler,
                                                                                                  operatorInputSchema,
                                                                                                  operatorOutputSchema);
            parentOperator->setChild(flatMapJavaUDF);
            return flatMapJavaUDF;
        }
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
        auto handler =
            std::make_shared<Runtime::Execution::Operators::NonKeyedThresholdWindowOperatorHandler>(std::move(aggValues));
        operatorHandlers.push_back(handler);
        auto indexForThisHandler = operatorHandlers.size() - 1;

        auto thresholdWindow = lowerThresholdWindow(pipeline, operatorNode, indexForThisHandler);
        parentOperator->setChild(thresholdWindow);
        return thresholdWindow;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalNonKeyedThreadLocalPreAggregationOperator>()) {
        auto preAggregationOperator = lowerGlobalThreadLocalPreAggregationOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(preAggregationOperator);
        return preAggregationOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator>()) {
        auto preAggregationOperator = lowerKeyedThreadLocalPreAggregationOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(preAggregationOperator);
        return preAggregationOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalNonKeyedSliceMergingOperator>()) {
        return lowerNonKeyedSliceMergingOperator(pipeline, operatorNode, operatorHandlers);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedSliceMergingOperator>()) {
        return lowerKeyedSliceMergingOperator(pipeline, operatorNode, operatorHandlers);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalNonKeyedTumblingWindowSink>()) {
        // As the sink is already part of the slice merging, we can ignore this operator for now.
        return parentOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedTumblingWindowSink>()) {
        //  As the sink is already part of the slice merging,  we can ignore this operator for now.
        return parentOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedSlidingWindowSink>()) {
        return lowerKeyedSlidingWindowSinkOperator(pipeline, operatorNode, operatorHandlers);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalNonKeyedWindowSliceStoreAppendOperator>()
               || operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedGlobalSliceStoreAppendOperator>()) {
        //  As the append operator is already part of the slice merging,  we can ignore this operator for now.
        return parentOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalNonKeyedSlidingWindowSink>()) {
        return lowerNonKeyedSlidingWindowSinkOperator(pipeline, operatorNode, operatorHandlers);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>()) {
        auto watermarkOperator = lowerWatermarkAssignmentOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(watermarkOperator);
        return watermarkOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalProjectOperator>()) {
        auto projectOperator = operatorNode->as<PhysicalOperators::PhysicalProjectOperator>();
        auto projection =
            std::make_shared<Runtime::Execution::Operators::Project>(projectOperator->getInputSchema()->getFieldNames(),
                                                                     projectOperator->getOutputSchema()->getFieldNames());
        parentOperator->setChild(projection);
        return projection;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalHashJoinProbeOperator>()) {
        auto sinkOperator = operatorNode->as<PhysicalOperators::PhysicalHashJoinProbeOperator>();
        NES_DEBUG("Added streamJoinOpHandler to operatorHandlers!");
        operatorHandlers.push_back(sinkOperator->getOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto joinSinkNautilus = std::make_shared<Runtime::Execution::Operators::StreamHashJoinProbe>(
            handlerIndex,
            sinkOperator->getLeftInputSchema(),
            sinkOperator->getRightInputSchema(),
            sinkOperator->NES::BinaryOperatorNode::getOutputSchema(),
            sinkOperator->getJoinFieldNameLeft(),
            sinkOperator->getJoinFieldNameRight());
        pipeline.setRootOperator(joinSinkNautilus);
        return joinSinkNautilus;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalNestedLoopJoinProbeOperator>()) {
        const auto sinkOperator = operatorNode->as<PhysicalOperators::PhysicalNestedLoopJoinProbeOperator>();

        operatorHandlers.push_back(sinkOperator->getOperatorHandler());
        const auto handlerIndex = operatorHandlers.size() - 1;
        const auto leftInputSchema = sinkOperator->getLeftInputSchema();
        const auto rightInputSchema = sinkOperator->getRightInputSchema();

        auto joinSinkNautilus =
            std::make_shared<Runtime::Execution::Operators::NLJProbe>(handlerIndex,
                                                                      leftInputSchema,
                                                                      rightInputSchema,
                                                                      sinkOperator->NES::BinaryOperatorNode::getOutputSchema(),
                                                                      leftInputSchema->getSchemaSizeInBytes(),
                                                                      rightInputSchema->getSchemaSizeInBytes(),
                                                                      sinkOperator->getJoinFieldNameLeft(),
                                                                      sinkOperator->getJoinFieldNameRight(),
                                                                      sinkOperator->getWindowStartFieldName(),
                                                                      sinkOperator->getWindowEndFieldName(),
                                                                      sinkOperator->getWindowKeyFieldName());
        pipeline.setRootOperator(joinSinkNautilus);
        return joinSinkNautilus;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalHashJoinBuildOperator>()) {
        auto buildOperator = operatorNode->as<PhysicalOperators::PhysicalHashJoinBuildOperator>();

        NES_DEBUG("Added streamJoinOpHandler to operatorHandlers!");
        operatorHandlers.push_back(buildOperator->getOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;
        //
        std::shared_ptr<Runtime::Execution::Operators::StreamHashJoinBuild> joinBuildNautilus;
        if (buildOperator->getTimeStampFieldName() == "IngestionTime") {
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
            joinBuildNautilus =
                std::make_shared<Runtime::Execution::Operators::StreamHashJoinBuild>(handlerIndex,
                                                                                     buildOperator->getBuildSide(),
                                                                                     buildOperator->getJoinFieldName(),
                                                                                     buildOperator->getTimeStampFieldName(),
                                                                                     buildOperator->getInputSchema(),
                                                                                     std::move(timeFunction));
        } else {
            auto timeStampFieldRecord =
                std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(buildOperator->getTimeStampFieldName());
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(timeStampFieldRecord);
            joinBuildNautilus =
                std::make_shared<Runtime::Execution::Operators::StreamHashJoinBuild>(handlerIndex,
                                                                                     buildOperator->getBuildSide(),
                                                                                     buildOperator->getJoinFieldName(),
                                                                                     buildOperator->getTimeStampFieldName(),
                                                                                     buildOperator->getInputSchema(),
                                                                                     std::move(timeFunction));
        }

        parentOperator->setChild(std::dynamic_pointer_cast<Runtime::Execution::Operators::ExecutableOperator>(joinBuildNautilus));
        return joinBuildNautilus;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalNestedLoopJoinBuildOperator>()) {
        auto buildOperator = operatorNode->as<PhysicalOperators::PhysicalNestedLoopJoinBuildOperator>();

        NES_DEBUG("Added PhysicalNestedLoopJoinBuildOperator to operatorHandlers!");
        operatorHandlers.push_back(buildOperator->getOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto tsField = buildOperator->getTimeStampFieldName();
        auto joinBuildSide = buildOperator->getBuildSide();
        std::shared_ptr<Runtime::Execution::Operators::NLJBuild> joinBuildNautilus;

        if (buildOperator->getTimeStampFieldName() == "IngestionTime") {
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
            joinBuildNautilus = std::make_shared<Runtime::Execution::Operators::NLJBuild>(handlerIndex,
                                                                                          buildOperator->getInputSchema(),
                                                                                          buildOperator->getJoinFieldName(),
                                                                                          buildOperator->getTimeStampFieldName(),
                                                                                          joinBuildSide,
                                                                                          std::move(timeFunction));
        } else {
            auto timeStampFieldRecord =
                std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(buildOperator->getTimeStampFieldName());
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(timeStampFieldRecord);
            joinBuildNautilus = std::make_shared<Runtime::Execution::Operators::NLJBuild>(handlerIndex,
                                                                                          buildOperator->getInputSchema(),
                                                                                          buildOperator->getJoinFieldName(),
                                                                                          buildOperator->getTimeStampFieldName(),
                                                                                          joinBuildSide,
                                                                                          std::move(timeFunction));
        }

        parentOperator->setChild(std::dynamic_pointer_cast<Runtime::Execution::Operators::ExecutableOperator>(joinBuildNautilus));
        return joinBuildNautilus;
    }
#ifdef TFDEF
    else if (operatorNode->instanceOf<PhysicalOperators::PhysicalInferModelOperator>()) {
        auto inferModel = lowerInferModelOperator(operatorNode, operatorHandlers);
        parentOperator->setChild(inferModel);
        return inferModel;
    }
#endif
    NES_NOT_IMPLEMENTED();
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerKeyedSlidingWindowSinkOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalSWS = physicalOperator->as<PhysicalOperators::PhysicalKeyedSlidingWindowSink>();

    auto aggregations = physicalSWS->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    // We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    // TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalSWS->getOutputSchema()->get(0)->getName();
    auto endTs = physicalSWS->getOutputSchema()->get(1)->getName();
    auto windowType = physicalSWS->getWindowDefinition()->getWindowType();
    auto keys = physicalSWS->getWindowDefinition()->getKeys();
    std::vector<std::string> resultKeyFields;
    std::vector<PhysicalTypePtr> resultKeyDataTypes;
    for (const auto& key : keys) {
        resultKeyFields.emplace_back(key->getFieldName());
        resultKeyDataTypes.emplace_back(DefaultPhysicalTypeFactory().getPhysicalType(key->getStamp()));
    }
    uint64_t keySize = 0;
    uint64_t valueSize = 0;
    for (auto& keyType : resultKeyDataTypes) {
        keySize = keySize + keyType->size();
    }
    for (auto& function : aggregationFunctions) {
        valueSize = valueSize + function->getSize();
    }

    std::unique_ptr<Runtime::Execution::Operators::SliceMergingAction> sliceMergingAction =
        std::make_unique<Runtime::Execution::Operators::KeyedWindowEmitAction>(aggregationFunctions,
                                                                               startTs,
                                                                               endTs,
                                                                               keySize,
                                                                               valueSize,
                                                                               resultKeyFields,
                                                                               resultKeyDataTypes,
                                                                               physicalSWS->getWindowDefinition()->getOriginId());
    auto handler = std::make_shared<Runtime::Execution::Operators::KeyedSliceMergingHandler>();
    operatorHandlers.emplace_back(handler);
    auto sliceMergingOperator = std::make_shared<Runtime::Execution::Operators::KeyedSliceMerging>(operatorHandlers.size() - 1,
                                                                                                   aggregationFunctions,
                                                                                                   std::move(sliceMergingAction),
                                                                                                   resultKeyDataTypes,
                                                                                                   keySize,
                                                                                                   valueSize);

    pipeline.setRootOperator(sliceMergingOperator);
    return sliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerNonKeyedSlidingWindowSinkOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalSWS = physicalOperator->as<PhysicalOperators::PhysicalNonKeyedSlidingWindowSink>();

    auto aggregations = physicalSWS->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    // We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    // TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalSWS->getOutputSchema()->get(0)->getName();
    auto endTs = physicalSWS->getOutputSchema()->get(1)->getName();
    auto windowType = physicalSWS->getWindowDefinition()->getWindowType();
    // TODO refactor operator selection
    auto isTumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType) != nullptr ? true : false;
    std::unique_ptr<Runtime::Execution::Operators::SliceMergingAction> sliceMergingAction =
        std::make_unique<Runtime::Execution::Operators::NonKeyedWindowEmitAction>(
            aggregationFunctions,
            startTs,
            endTs,
            physicalSWS->getWindowDefinition()->getOriginId());

    auto handler = std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMergingHandler>();
    operatorHandlers.emplace_back(handler);
    auto nonKeyedSliceMergingOperator =
        std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMerging>(operatorHandlers.size() - 1,
                                                                              aggregationFunctions,
                                                                              std::move(sliceMergingAction));

    pipeline.setRootOperator(nonKeyedSliceMergingOperator);
    return nonKeyedSliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerNonKeyedSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGSMO = physicalOperator->as<PhysicalOperators::PhysicalNonKeyedSliceMergingOperator>();
    auto handler =
        std::get<std::shared_ptr<Runtime::Execution::Operators::NonKeyedSliceMergingHandler>>(physicalGSMO->getWindowHandler());
    operatorHandlers.emplace_back(handler);
    auto sliceMergingOperatorHandlerIndex = operatorHandlers.size() - 1;
    auto aggregations = physicalGSMO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    // We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    // TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.

    // TODO refactor operator selection
    auto windowType = physicalGSMO->getWindowDefinition()->getWindowType();
    auto isTumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType) != nullptr ? true : false;
    auto startTs = physicalGSMO->getOutputSchema()->get(0)->getName();
    auto endTs = physicalGSMO->getOutputSchema()->get(1)->getName();

    std::unique_ptr<Runtime::Execution::Operators::SliceMergingAction> sliceMergingAction;
    if (isTumblingWindow || options->getWindowingStrategy() == QueryCompilerOptions::WindowingStrategy::BUCKET) {
        sliceMergingAction = std::make_unique<Runtime::Execution::Operators::NonKeyedWindowEmitAction>(
            aggregationFunctions,
            startTs,
            endTs,
            physicalGSMO->getWindowDefinition()->getOriginId());
    } else {
        auto timeBasedWindowType =
            Windowing::WindowType::asTimeBasedWindowType(physicalGSMO->getWindowDefinition()->getWindowType());
        auto windowSize = timeBasedWindowType->getSize().getTime();
        auto windowSlide = timeBasedWindowType->getSlide().getTime();
        auto actionHandler =
            std::make_shared<Runtime::Execution::Operators::NonKeyedAppendToSliceStoreHandler>(windowSize, windowSlide);
        operatorHandlers.emplace_back(actionHandler);
        sliceMergingAction =
            std::make_unique<Runtime::Execution::Operators::NonKeyedAppendToSliceStoreAction>(operatorHandlers.size() - 1);
    }
    auto nonKeyedSliceMergingOperator =
        std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMerging>(sliceMergingOperatorHandlerIndex,
                                                                              aggregationFunctions,
                                                                              std::move(sliceMergingAction));

    pipeline.setRootOperator(nonKeyedSliceMergingOperator);
    return nonKeyedSliceMergingOperator;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerKeyedSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGSMO = physicalOperator->as<PhysicalOperators::PhysicalKeyedSliceMergingOperator>();
    auto handler =
        std::get<std::shared_ptr<Runtime::Execution::Operators::KeyedSliceMergingHandler>>(physicalGSMO->getWindowHandler());
    operatorHandlers.emplace_back(handler);
    auto sliceMergingOperatorHandlerIndex = operatorHandlers.size() - 1;
    auto aggregations = physicalGSMO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    // We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    // TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalGSMO->getOutputSchema()->get(0)->getName();
    auto endTs = physicalGSMO->getOutputSchema()->get(1)->getName();
    auto keys = physicalGSMO->getWindowDefinition()->getKeys();

    std::vector<std::string> resultKeyFields;
    std::vector<PhysicalTypePtr> resultKeyDataTypes;
    for (const auto& key : keys) {
        resultKeyFields.emplace_back(key->getFieldName());
        resultKeyDataTypes.emplace_back(DefaultPhysicalTypeFactory().getPhysicalType(key->getStamp()));
    }
    uint64_t keySize = 0;
    uint64_t valueSize = 0;
    for (auto& keyType : resultKeyDataTypes) {
        keySize = keySize + keyType->size();
    }
    for (auto& function : aggregationFunctions) {
        valueSize = valueSize + function->getSize();
    }
    auto windowType = physicalGSMO->getWindowDefinition()->getWindowType();
    auto isTumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType) != nullptr ? true : false;

    std::unique_ptr<Runtime::Execution::Operators::SliceMergingAction> sliceMergingAction;
    if (isTumblingWindow || options->getWindowingStrategy() == QueryCompilerOptions::WindowingStrategy::BUCKET) {
        sliceMergingAction = std::make_unique<Runtime::Execution::Operators::KeyedWindowEmitAction>(
            aggregationFunctions,
            startTs,
            endTs,
            keySize,
            valueSize,
            resultKeyFields,
            resultKeyDataTypes,
            physicalGSMO->getWindowDefinition()->getOriginId());
    } else {
        auto timeBasedWindowType =
            Windowing::WindowType::asTimeBasedWindowType(physicalGSMO->getWindowDefinition()->getWindowType());
        auto windowSize = timeBasedWindowType->getSize().getTime();
        auto windowSlide = timeBasedWindowType->getSlide().getTime();
        auto actionHandler =
            std::make_shared<Runtime::Execution::Operators::KeyedAppendToSliceStoreHandler>(windowSize, windowSlide);
        operatorHandlers.emplace_back(actionHandler);
        sliceMergingAction =
            std::make_unique<Runtime::Execution::Operators::KeyedAppendToSliceStoreAction>(operatorHandlers.size() - 1);
    }

    auto sliceMergingOperator =
        std::make_shared<Runtime::Execution::Operators::KeyedSliceMerging>(sliceMergingOperatorHandlerIndex,
                                                                           aggregationFunctions,
                                                                           std::move(sliceMergingAction),
                                                                           resultKeyDataTypes,
                                                                           keySize,
                                                                           valueSize);
    pipeline.setRootOperator(sliceMergingOperator);
    return sliceMergingOperator;
}

std::unique_ptr<Runtime::Execution::Operators::TimeFunction>
LowerPhysicalToNautilusOperators::lowerTimeFunction(const Windowing::TimeBasedWindowTypePtr& timeWindow) {
    // Depending on the window type we create a different time function.
    // If the window type is ingestion time or we use the special record creation ts field, create an ingestion time function.
    // TODO remove record creation ts if it is not needed anymore
    if (timeWindow->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::IngestionTime
        || timeWindow->getTimeCharacteristic()->getField()->getName()
            == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME) {
        return std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
    } else if (timeWindow->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
        // For event time fields, we look up the reference field name and create an expression to read the field.
        auto timeCharacteristicField = timeWindow->getTimeCharacteristic()->getField()->getName();
        auto timeStampField = std::make_shared<Runtime::Execution::Expressions::ReadFieldExpression>(timeCharacteristicField);
        return std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(timeStampField);
    }
    NES_THROW_RUNTIME_ERROR("Timefunction could not be created for the following window definition: " << timeWindow->toString());
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerGlobalThreadLocalPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGTLPAO = physicalOperator->as<PhysicalOperators::PhysicalNonKeyedThreadLocalPreAggregationOperator>();
    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = physicalGTLPAO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    auto timeWindow = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto timeFunction = lowerTimeFunction(timeWindow);

    if (options->getWindowingStrategy() == QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL) {
        auto handler = std::get<std::shared_ptr<Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler>>(
            physicalGTLPAO->getWindowHandler());
        operatorHandlers.emplace_back(handler);
        auto slicePreAggregation =
            std::make_shared<Runtime::Execution::Operators::NonKeyedSlicePreAggregation>(operatorHandlers.size() - 1,
                                                                                         std::move(timeFunction),
                                                                                         aggregationFunctions);
        return slicePreAggregation;
    } else if (options->getWindowingStrategy() == QueryCompilerOptions::WindowingStrategy::BUCKET) {
        auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());

        auto handler = std::get<std::shared_ptr<Runtime::Execution::Operators::NonKeyedBucketPreAggregationHandler>>(
            physicalGTLPAO->getWindowHandler());
        operatorHandlers.emplace_back(handler);
        auto bucketPreAggregation =
            std::make_shared<Runtime::Execution::Operators::NonKeyedBucketPreAggregation>(operatorHandlers.size() - 1,
                                                                                          std::move(timeFunction),
                                                                                          aggregationFunctions);
        return bucketPreAggregation;
    }
    NES_NOT_IMPLEMENTED();
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerKeyedThreadLocalPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGTLPAO = physicalOperator->as<PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator>();

    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = windowDefinition->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    auto timeWindow = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    auto timeFunction = lowerTimeFunction(timeWindow);
    auto keys = windowDefinition->getKeys();
    NES_ASSERT(!keys.empty(), "A keyed window should have keys");
    std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyReadExpressions;
    auto df = DefaultPhysicalTypeFactory();
    std::vector<PhysicalTypePtr> keyDataTypes;
    for (const auto& key : keys) {
        keyReadExpressions.emplace_back(expressionProvider->lowerExpression(key));
        keyDataTypes.emplace_back(df.getPhysicalType(key->getStamp()));
    }

    if (options->getWindowingStrategy() == QueryCompilerOptions::WindowingStrategy::THREAD_LOCAL) {
        auto handler = std::get<std::shared_ptr<Runtime::Execution::Operators::KeyedSlicePreAggregationHandler>>(
            physicalGTLPAO->getWindowHandler());
        operatorHandlers.emplace_back(handler);
        auto sliceMergingOperator = std::make_shared<Runtime::Execution::Operators::KeyedSlicePreAggregation>(
            operatorHandlers.size() - 1,
            std::move(timeFunction),
            keyReadExpressions,
            keyDataTypes,
            aggregationFunctions,
            std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        return sliceMergingOperator;
    } else if (options->getWindowingStrategy() == QueryCompilerOptions::WindowingStrategy::BUCKET) {
        auto handler = std::get<std::shared_ptr<Runtime::Execution::Operators::KeyedBucketPreAggregationHandler>>(
            physicalGTLPAO->getWindowHandler());
        operatorHandlers.emplace_back(handler);
        auto bucketPreAggregation = std::make_shared<Runtime::Execution::Operators::KeyedBucketPreAggregation>(
            operatorHandlers.size() - 1,
            std::move(timeFunction),
            keyReadExpressions,
            keyDataTypes,
            aggregationFunctions,
            std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        return bucketPreAggregation;
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerWatermarkAssignmentOperator(Runtime::Execution::PhysicalOperatorPipeline&,
                                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
                                                                   std::vector<Runtime::Execution::OperatorHandlerPtr>&) {
    auto wao = operatorPtr->as<PhysicalOperators::PhysicalWatermarkAssignmentOperator>();

    //Add either event time or ingestion time watermark strategy
    if (wao->getWatermarkStrategyDescriptor()->instanceOf<Windowing::EventTimeWatermarkStrategyDescriptor>()) {
        auto eventTimeWatermarkStrategy =
            wao->getWatermarkStrategyDescriptor()->as<Windowing::EventTimeWatermarkStrategyDescriptor>();
        auto fieldExpression = expressionProvider->lowerExpression(eventTimeWatermarkStrategy->getOnField());
        auto watermarkAssignmentOperator = std::make_shared<Runtime::Execution::Operators::EventTimeWatermarkAssignment>(
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(fieldExpression));
        return watermarkAssignmentOperator;
    } else if (wao->getWatermarkStrategyDescriptor()->instanceOf<Windowing::IngestionTimeWatermarkStrategyDescriptor>()) {
        auto watermarkAssignmentOperator = std::make_shared<Runtime::Execution::Operators::IngestionTimeWatermarkAssignment>(
            std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>());
        return watermarkAssignmentOperator;
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lowerScan(Runtime::Execution::PhysicalOperatorPipeline&,
                                            const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
                                            size_t bufferSize) {
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only row layout is supported");
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
    NES_ASSERT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only row layout is supported");
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
LowerPhysicalToNautilusOperators::lowerLimit(Runtime::Execution::PhysicalOperatorPipeline&,
                                             const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
                                             std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto limitOperator = operatorPtr->as<PhysicalOperators::PhysicalLimitOperator>();
    const auto handler = std::make_shared<Runtime::Execution::Operators::LimitOperatorHandler>(limitOperator->getLimit());
    operatorHandlers.push_back(handler);
    return std::make_shared<Runtime::Execution::Operators::Limit>(operatorHandlers.size() - 1);
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
    NES_INFO("lowerThresholdWindow {} and handlerid {}", operatorPtr->toString(), handlerIndex);
    auto thresholdWindowOperator = operatorPtr->as<PhysicalOperators::PhysicalThresholdWindowOperator>();
    auto contentBasedWindowType = Windowing::ContentBasedWindowType::asContentBasedWindowType(
        thresholdWindowOperator->getOperatorHandler()->getWindowDefinition()->getWindowType());
    auto thresholdWindowType = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
    NES_INFO("lowerThresholdWindow Predicate {}", thresholdWindowType->getPredicate()->toString());
    auto predicate = expressionProvider->lowerExpression(thresholdWindowType->getPredicate());
    auto minCount = thresholdWindowType->getMinimumCount();

    auto aggregations = thresholdWindowOperator->getOperatorHandler()->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    std::vector<std::string> aggregationResultFieldNames;
    std::transform(aggregations.cbegin(),
                   aggregations.cend(),
                   std::back_inserter(aggregationResultFieldNames),
                   [&](const Windowing::WindowAggregationDescriptorPtr& agg) {
                       return agg->as()->as_if<FieldAccessExpressionNode>()->getFieldName();
                   });

    return std::make_shared<Runtime::Execution::Operators::NonKeyedThresholdWindow>(predicate,
                                                                                    aggregationResultFieldNames,
                                                                                    minCount,
                                                                                    aggregationFunctions,
                                                                                    handlerIndex);
}

std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>>
LowerPhysicalToNautilusOperators::lowerAggregations(const std::vector<Windowing::WindowAggregationPtr>& aggs) {
    NES_INFO("Lower Window Aggregations to Nautilus Operator");
    std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>> aggregationFunctions;
    std::transform(aggs.cbegin(),
                   aggs.cend(),
                   std::back_inserter(aggregationFunctions),
                   [&](const Windowing::WindowAggregationDescriptorPtr& agg)
                       -> std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction> {
                       DefaultPhysicalTypeFactory physicalTypeFactory = DefaultPhysicalTypeFactory();

                       // lower the data types
                       auto physicalInputType = physicalTypeFactory.getPhysicalType(agg->getInputStamp());
                       auto physicalFinalType = physicalTypeFactory.getPhysicalType(agg->getFinalAggregateStamp());

                       auto aggregationInputExpression = expressionProvider->lowerExpression(agg->on());
                       std::string aggregationResultFieldIdentifier;
                       if (auto fieldAccessExpression = agg->as()->as_if<FieldAccessExpressionNode>()) {
                           aggregationResultFieldIdentifier = fieldAccessExpression->getFieldName();
                       } else {
                           NES_THROW_RUNTIME_ERROR("Currently complex expression in as fields are not supported");
                       }
                       switch (agg->getType()) {
                           case Windowing::WindowAggregationDescriptor::Type::Avg:
                               return std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(
                                   physicalInputType,
                                   physicalFinalType,
                                   aggregationInputExpression,
                                   aggregationResultFieldIdentifier);
                           case Windowing::WindowAggregationDescriptor::Type::Count:
                               return std::make_shared<Runtime::Execution::Aggregation::CountAggregationFunction>(
                                   physicalInputType,
                                   physicalFinalType,
                                   aggregationInputExpression,
                                   aggregationResultFieldIdentifier);
                           case Windowing::WindowAggregationDescriptor::Type::Max:
                               return std::make_shared<Runtime::Execution::Aggregation::MaxAggregationFunction>(
                                   physicalInputType,
                                   physicalFinalType,
                                   aggregationInputExpression,
                                   aggregationResultFieldIdentifier);
                           case Windowing::WindowAggregationDescriptor::Type::Min:
                               return std::make_shared<Runtime::Execution::Aggregation::MinAggregationFunction>(
                                   physicalInputType,
                                   physicalFinalType,
                                   aggregationInputExpression,
                                   aggregationResultFieldIdentifier);
                           case Windowing::WindowAggregationDescriptor::Type::Median:
                               // TODO 3331: add median aggregation function
                               break;
                           case Windowing::WindowAggregationDescriptor::Type::Sum: {
                               return std::make_shared<Runtime::Execution::Aggregation::SumAggregationFunction>(
                                   physicalInputType,
                                   physicalFinalType,
                                   aggregationInputExpression,
                                   aggregationResultFieldIdentifier);
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
        case Windowing::WindowAggregationDescriptor::Type::Avg:
            switch (basicType->nativeType) {
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
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        case Windowing::WindowAggregationDescriptor::Type::Count:
            switch (basicType->nativeType) {
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
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        case Windowing::WindowAggregationDescriptor::Type::Max:
            switch (basicType->nativeType) {
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
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        case Windowing::WindowAggregationDescriptor::Type::Min:
            switch (basicType->nativeType) {
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
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        case Windowing::WindowAggregationDescriptor::Type::Sum:
            switch (basicType->nativeType) {
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
                default: NES_THROW_RUNTIME_ERROR("Unsupported data type");
            }
        default: NES_THROW_RUNTIME_ERROR("Unsupported aggregation type");
    }
}

#ifdef TFDEF
std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerInferModelOperator(const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
                                                          std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {

    auto inferModelOperator = physicalOperator->as<PhysicalOperators::PhysicalInferModelOperator>();
    auto model = inferModelOperator->getModel();

    //Fetch the name of input fields
    std::vector<std::string> inputFields;
    for (const auto& inputField : inferModelOperator->getInputFields()) {
        auto fieldAccessExpression = inputField->getExpressionNode()->as<FieldAccessExpressionNode>();
        inputFields.push_back(fieldAccessExpression->getFieldName());
    }

    //Fetch the name of output fields
    std::vector<std::string> outputFields;
    for (const auto& outputField : inferModelOperator->getOutputFields()) {
        auto fieldAccessExpression = outputField->getExpressionNode()->as<FieldAccessExpressionNode>();
        outputFields.push_back(fieldAccessExpression->getFieldName());
    }

    //build the handler to invoke model during execution
    auto handler = std::make_shared<Runtime::Execution::Operators::InferModelHandler>(model);
    operatorHandlers.push_back(handler);
    auto indexForThisHandler = operatorHandlers.size() - 1;

    //build nautilus infer model operator
    return std::make_shared<Runtime::Execution::Operators::InferModelOperator>(indexForThisHandler, inputFields, outputFields);
}
#endif

LowerPhysicalToNautilusOperators::~LowerPhysicalToNautilusOperators() = default;

}// namespace NES::QueryCompilation