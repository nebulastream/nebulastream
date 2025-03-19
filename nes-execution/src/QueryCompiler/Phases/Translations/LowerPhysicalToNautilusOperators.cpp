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

#include <API/Schema.hpp>
#include <API/TimeUnit.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
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
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedWindowEmitAction.hpp>
#include <Execution/Operators/Streaming/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/IngestionTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Bucketing/HJBuildBucketing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbe.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbeVarSized.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJBuildSlicingVarSized.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJBuildLeft.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJBuildRight.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Bucketing/NLJBuildBucketing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/HyperLogLog/HyperLogLogBuild.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/HyperLogLog/HyperLogLogOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindow.hpp>
#include <Execution/Operators/ThresholdWindow/NonKeyedThresholdWindow/NonKeyedThresholdWindowOperatorHandler.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Expressions/FieldAssignmentExpressionNode.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/IntervalJoin/PhysicalIntervalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/IntervalJoin/PhysicalIntervalJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>
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
#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalCountMinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/StatisticCollection/PhysicalHyperLogLogBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/ContentBasedWindow/PhysicalThresholdWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/ExpressionProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Formats/StatisticCollection/CountMinStatisticFormat.hpp>
#include <Sinks/Formats/StatisticCollection/HyperLogLogStatisticFormat.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <Types/ContentBasedWindowType.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/CompilerConstants.hpp>
#include <Util/Core.hpp>
#include <Util/Execution.hpp>
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
    auto decomposedQueryPlan = operatorPipeline->getDecomposedQueryPlan();
    auto nodes = PlanIterator(decomposedQueryPlan).snapshot();
    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;
    std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator;

    for (const auto& node : nodes) {
        NES_INFO("Lowering node: {}", node->toString());
        parentOperator =
            lower(*pipeline, parentOperator, node->as<PhysicalOperators::PhysicalOperator>(), bufferSize, operatorHandlers);
        parentOperator->setStatisticId(node->as<PhysicalOperators::PhysicalOperator>()->getStatisticId());
    }
    const auto& rootOperators = decomposedQueryPlan->getRootOperators();
    for (auto& root : rootOperators) {
        decomposedQueryPlan->removeAsRootOperator(root->getId());
    }
    auto nautilusPipelineWrapper = NautilusPipelineOperator::create(pipeline, operatorHandlers);
    decomposedQueryPlan->addRootOperator(nautilusPipelineWrapper);
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
        auto aggs =
            operatorNode->as<PhysicalOperators::PhysicalThresholdWindowOperator>()->getWindowDefinition()->getWindowAggregation();

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
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSlicePreAggregationOperator>()) {
        auto preAggregationOperator = lowerPreAggregationOperator(pipeline, operatorNode, operatorHandlers);
        parentOperator->setChild(preAggregationOperator);
        return preAggregationOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSliceMergingOperator>()) {
        return lowerSliceMergingOperator(pipeline, operatorNode, operatorHandlers);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalWindowSinkOperator>()) {
        return lowerWindowSinkOperator(pipeline, operatorNode, operatorHandlers);
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
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalStreamJoinProbeOperator>()) {
        auto probeOperator = operatorNode->as<PhysicalOperators::PhysicalStreamJoinProbeOperator>();
        NES_DEBUG("Added streamJoinOpHandler to operatorHandlers!");
        operatorHandlers.push_back(probeOperator->getJoinOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto keys = probeOperator->getJoinExpression();
        auto joinExpression = expressionProvider->lowerExpression(keys);

        Runtime::Execution::Operators::OperatorPtr joinProbeNautilus;
        switch (probeOperator->getJoinStrategy()) {
            case StreamJoinStrategy::HASH_JOIN_VAR_SIZED:
                joinProbeNautilus = std::make_shared<Runtime::Execution::Operators::HJProbeVarSized>(
                    handlerIndex,
                    probeOperator->getJoinSchema(),
                    joinExpression,
                    probeOperator->getWindowMetaData(),
                    probeOperator->getLeftInputSchema(),
                    probeOperator->getRightInputSchema(),
                    probeOperator->getJoinStrategy(),
                    probeOperator->getWindowingStrategy(),
                    probeOperator->getTimestampFieldLeft().toTimeFunction(),
                    probeOperator->getTimestampFieldRight().toTimeFunction());
                break;
            case StreamJoinStrategy::HASH_JOIN_LOCAL:
            case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING:
            case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE:
                joinProbeNautilus = std::make_shared<Runtime::Execution::Operators::HJProbe>(
                    handlerIndex,
                    probeOperator->getJoinSchema(),
                    joinExpression,
                    probeOperator->getWindowMetaData(),
                    probeOperator->getJoinStrategy(),
                    probeOperator->getWindowingStrategy(),
                    probeOperator->getTimestampFieldLeft().toTimeFunction(),
                    probeOperator->getTimestampFieldRight().toTimeFunction());
                break;
            case StreamJoinStrategy::NESTED_LOOP_JOIN: {
                const auto leftSchema = probeOperator->getLeftInputSchema();
                const auto rightSchema = probeOperator->getRightInputSchema();
                joinProbeNautilus = std::make_shared<Runtime::Execution::Operators::NLJProbe>(
                    handlerIndex,
                    probeOperator->getJoinSchema(),
                    joinExpression,
                    probeOperator->getWindowMetaData(),
                    leftSchema,
                    rightSchema,
                    probeOperator->getJoinStrategy(),
                    probeOperator->getWindowingStrategy(),
                    probeOperator->getTimestampFieldLeft().toTimeFunction(),
                    probeOperator->getTimestampFieldRight().toTimeFunction());
                break;
            }
        }
        pipeline.setRootOperator(joinProbeNautilus);
        return joinProbeNautilus;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalStreamJoinBuildOperator>()) {
        using namespace Runtime::Execution;

        auto buildOperator = operatorNode->as<PhysicalOperators::PhysicalStreamJoinBuildOperator>();
        auto buildOperatorHandler = buildOperator->getJoinOperatorHandler();

        NES_DEBUG("Added streamJoinOpHandler to operatorHandlers!");
        operatorHandlers.push_back(buildOperator->getJoinOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;
        auto windowSize = buildOperatorHandler->getWindowSize();
        auto windowSlide = buildOperatorHandler->getWindowSlide();

        auto timeFunction = buildOperator->getTimeStampField().toTimeFunction();

        Operators::ExecutableOperatorPtr joinBuildNautilus;
        switch (buildOperator->getJoinStrategy()) {
            case StreamJoinStrategy::HASH_JOIN_VAR_SIZED:
                switch (buildOperator->getWindowingStrategy()) {
                    case WindowingStrategy::LEGACY:
                    case WindowingStrategy::BUCKETING: NES_NOT_IMPLEMENTED();
                    case WindowingStrategy::SLICING:
                        joinBuildNautilus = lowerHJSlicingVarSized(buildOperator, handlerIndex, std::move(timeFunction));
                        break;
                }
                break;
            case StreamJoinStrategy::HASH_JOIN_LOCAL:
            case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCKING:
            case StreamJoinStrategy::HASH_JOIN_GLOBAL_LOCK_FREE: {
                switch (buildOperator->getWindowingStrategy()) {
                    case WindowingStrategy::LEGACY: NES_NOT_IMPLEMENTED();
                    case WindowingStrategy::SLICING:
                        joinBuildNautilus = lowerHJSlicing(buildOperator, handlerIndex, std::move(timeFunction));
                        break;
                    case WindowingStrategy::BUCKETING:
                        joinBuildNautilus =
                            lowerHJBucketing(buildOperator, handlerIndex, std::move(timeFunction), windowSize, windowSlide);
                        break;
                }
                break;
            };
            case StreamJoinStrategy::NESTED_LOOP_JOIN: {
                switch (buildOperator->getWindowingStrategy()) {
                    case WindowingStrategy::LEGACY: NES_NOT_IMPLEMENTED();
                    case WindowingStrategy::SLICING:
                        joinBuildNautilus = lowerNLJSlicing(buildOperator, handlerIndex, std::move(timeFunction));
                        break;
                    case WindowingStrategy::BUCKETING:
                        joinBuildNautilus =
                            lowerNLJBucketing(buildOperator, handlerIndex, std::move(timeFunction), windowSize, windowSlide);
                        break;
                }
                break;
            };
        }

        parentOperator->setChild(joinBuildNautilus);
        return joinBuildNautilus;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalIntervalJoinProbeOperator>()) {

        auto probeOperator = operatorNode->as<PhysicalOperators::PhysicalIntervalJoinProbeOperator>();

        NES_DEBUG("Added intervalJoinOpHandler to operatorHandlers!");
        operatorHandlers.push_back(probeOperator->getIJOperatorHandler());
        auto operatorHandlerIndex = operatorHandlers.size() - 1;
        auto keys = probeOperator->getJoinExpression();
        auto joinExpression = expressionProvider->lowerExpression(keys);

        Runtime::Execution::Operators::OperatorPtr joinProbeNautilus =
            std::make_shared<Runtime::Execution::Operators::IJProbe>(operatorHandlerIndex,
                                                                     joinExpression,
                                                                     probeOperator->getIntervalStartField(),
                                                                     probeOperator->getIntervalEndField(),
                                                                     probeOperator->getTimeStampField().toTimeFunction(),
                                                                     probeOperator->getLeftInputSchema(),
                                                                     probeOperator->getRightInputSchema());

        pipeline.setRootOperator(joinProbeNautilus);
        return joinProbeNautilus;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalIntervalJoinBuildOperator>()) {
        using namespace Runtime::Execution;

        auto buildOperator = operatorNode->as<PhysicalOperators::PhysicalIntervalJoinBuildOperator>();

        NES_DEBUG("Added intervalJoinOpHandler to operatorHandlers!");
        operatorHandlers.push_back(buildOperator->getIJOperatorHandler());
        auto operatorHandlerIndex = operatorHandlers.size() - 1;

        auto timeFunctionThisSide = buildOperator->getTimeStampFieldThisSide().toTimeFunction();
        auto timeFunctionOtherSide = buildOperator->getTimeStampFieldOtherSide().toTimeFunction();

        Operators::ExecutableOperatorPtr joinBuildNautilus;
        if (buildOperator->getBuildSide() == JoinBuildSideType::Left) {
            //the left build side needs to access the content of both left and right tuples, thus we provide timeFunctions for both
            joinBuildNautilus =
                std::make_shared<Runtime::Execution::Operators::IJBuildLeft>(operatorHandlerIndex,
                                                                             buildOperator->getInputSchema(),
                                                                             buildOperator->getOtherSideInputSchema(),
                                                                             std::move(timeFunctionThisSide),
                                                                             std::move(timeFunctionOtherSide));
        } else if (buildOperator->getBuildSide() == JoinBuildSideType::Right) {

            joinBuildNautilus =
                std::make_shared<Runtime::Execution::Operators::IJBuildRight>(operatorHandlerIndex,
                                                                              buildOperator->getInputSchema(),
                                                                              buildOperator->getOtherSideInputSchema(),
                                                                              std::move(timeFunctionThisSide));
        }

        parentOperator->setChild(joinBuildNautilus);
        return joinBuildNautilus;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalCountMinBuildOperator>()) {
        const auto physicalCountMinBuild = operatorNode->as<const PhysicalOperators::PhysicalCountMinBuildOperator>();
        auto countMinBuildOperator = lowerCountMinBuildOperator(*physicalCountMinBuild, operatorHandlers, bufferSize);
        parentOperator->setChild(countMinBuildOperator);
        return countMinBuildOperator;
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalHyperLogLogBuildOperator>()) {
        const auto physicalHyperLogLog = operatorNode->as<const PhysicalOperators::PhysicalHyperLogLogBuildOperator>();
        auto hyperLogLogBuildOperator = lowerHyperLogLogBuildOperator(*physicalHyperLogLog, operatorHandlers, bufferSize);
        parentOperator->setChild(hyperLogLogBuildOperator);
        return hyperLogLogBuildOperator;
    }

    // Check if a plugin is registered that handles this physical operator
    for (auto& plugin : NautilusOperatorLoweringPluginRegistry::getPlugins()) {
        auto resultOperator = plugin->lower(operatorNode, operatorHandlers);
        if (resultOperator.has_value()) {
            parentOperator->setChild(*resultOperator);
            return *resultOperator;
        }
    }

    NES_NOT_IMPLEMENTED();
}

Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerCountMinBuildOperator(
    const PhysicalOperators::PhysicalCountMinBuildOperator& physicalCountMinBuild,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers,
    uint64_t bufferSize) {
    using namespace Runtime::Execution::Operators;

    // 1. Getting all the necessary variables for the operator and its handler
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory;
    const auto fieldToTrackFieldName = physicalCountMinBuild.getNameOfFieldToTrack();
    const auto numberOfBitsInKey = 8
        * defaultPhysicalTypeFactory
              .getPhysicalType(physicalCountMinBuild.getInputSchema()->getField(fieldToTrackFieldName)->getDataType())
              ->size();
    const auto width = physicalCountMinBuild.getWidth();
    const auto depth = physicalCountMinBuild.getDepth();
    const auto metricHash = physicalCountMinBuild.getMetricHash();
    const auto outputMemoryLayout = ::NES::Util::createMemoryLayout(physicalCountMinBuild.getOutputSchema(), bufferSize);
    const auto inputOriginIds = physicalCountMinBuild.getInputOriginIds();
    const auto sendingPolicy = physicalCountMinBuild.getSendingPolicy();
    const auto sinkDataCodec = sendingPolicy->getSinkDataCodec();
    const auto statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(physicalCountMinBuild.getOutputSchema(),
                                                                                     bufferSize,
                                                                                     Statistic::StatisticSynopsisType::COUNT_MIN,
                                                                                     sinkDataCodec);

    // 2. Getting the windowSize, windowSlide, and timestampFieldName.
    const auto windowType = physicalCountMinBuild.getWindowType()->as<Windowing::TimeBasedWindowType>();
    NES_ASSERT(windowType->instanceOf<Windowing::TumblingWindow>() || windowType->instanceOf<Windowing::SlidingWindow>(),
               "Only a tumbling or sliding window is currently supported for CountMinBuildOperator");
    auto [windowSize, windowSlide, timeFunction] = Util::getWindowingParameters(*windowType);

    // 3. Create operator handler
    auto countMinBuildOperatorHandler = CountMinOperatorHandler::create(windowSize,
                                                                        windowSlide,
                                                                        sendingPolicy,
                                                                        width,
                                                                        depth,
                                                                        statisticFormat,
                                                                        inputOriginIds,
                                                                        numberOfBitsInKey);
    operatorHandlers.push_back(countMinBuildOperatorHandler);
    auto handlerIndex = operatorHandlers.size() - 1;

    // 4. Creating the operator
    return std::make_shared<CountMinBuild>(handlerIndex,
                                           fieldToTrackFieldName,
                                           numberOfBitsInKey,
                                           width,
                                           depth,
                                           metricHash,
                                           std::move(timeFunction));
}

Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerHyperLogLogBuildOperator(
    const PhysicalOperators::PhysicalHyperLogLogBuildOperator& physicalHLLBuildOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers,
    uint64_t bufferSize) {
    using namespace Runtime::Execution::Operators;

    // 1. Getting all the necessary variables for the operator and its handler
    const auto fieldToTrackFieldName = physicalHLLBuildOperator.getNameOfFieldToTrack();
    const auto width = physicalHLLBuildOperator.getWidth();
    const auto metricHash = physicalHLLBuildOperator.getMetricHash();
    const auto outputMemoryLayout = ::NES::Util::createMemoryLayout(physicalHLLBuildOperator.getOutputSchema(), bufferSize);
    const auto inputOriginIds = physicalHLLBuildOperator.getInputOriginIds();
    const auto sendingPolicy = physicalHLLBuildOperator.getSendingPolicy();
    const auto sinkDataCodec = sendingPolicy->getSinkDataCodec();
    const auto statisticFormat = Statistic::StatisticFormatFactory::createFromSchema(physicalHLLBuildOperator.getOutputSchema(),
                                                                                     bufferSize,
                                                                                     Statistic::StatisticSynopsisType::HLL,
                                                                                     sinkDataCodec);

    // 2. Getting the windowSize, windowSlide, and timestampFieldName. We will refactor this in #4739
    const auto windowType = physicalHLLBuildOperator.getWindowType()->as<Windowing::TimeBasedWindowType>();
    NES_ASSERT(windowType->instanceOf<Windowing::TumblingWindow>() || windowType->instanceOf<Windowing::SlidingWindow>(),
               "Only a tumbling or sliding window is currently supported for CountMinBuildOperator");
    auto [windowSize, windowSlide, timeFunction] = Util::getWindowingParameters(*windowType);

    // 3. Create operator handler
    auto hyperLogLogBuildOperatorHandler =
        HyperLogLogOperatorHandler::create(windowSize, windowSlide, sendingPolicy, statisticFormat, width, inputOriginIds);
    operatorHandlers.push_back(hyperLogLogBuildOperatorHandler);
    auto handlerIndex = operatorHandlers.size() - 1;

    // 4. Creating the operator
    return std::make_shared<HyperLogLogBuild>(handlerIndex, fieldToTrackFieldName, metricHash, std::move(timeFunction));
}

Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerNLJSlicing(
    std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator> buildOperator,
    uint64_t operatorHandlerIndex,
    Runtime::Execution::Operators::TimeFunctionPtr timeFunction) {
    return std::make_shared<Runtime::Execution::Operators::NLJBuildSlicing>(
        operatorHandlerIndex,
        buildOperator->getInputSchema(),
        buildOperator->getBuildSide(),
        buildOperator->getInputSchema()->getSchemaSizeInBytes(),
        std::move(timeFunction),
        buildOperator->getJoinStrategy());
}
Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerNLJBucketing(
    std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator> buildOperator,
    uint64_t operatorHandlerIndex,
    Runtime::Execution::Operators::TimeFunctionPtr timeFunction,
    uint64_t windowSize,
    uint64_t windowSlide) {
    return std::make_shared<Runtime::Execution::Operators::NLJBuildBucketing>(
        operatorHandlerIndex,
        buildOperator->getInputSchema(),
        buildOperator->getBuildSide(),
        buildOperator->getInputSchema()->getSchemaSizeInBytes(),
        std::move(timeFunction),
        buildOperator->getJoinStrategy(),
        windowSize,
        windowSlide);
}

Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerHJSlicing(
    const std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator>& buildOperator,
    uint64_t operatorHandlerIndex,
    Runtime::Execution::Operators::TimeFunctionPtr timeFunction) {
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
    Runtime::Execution::Operators::TimeFunctionPtr timeFunction) {
    return std::make_shared<Runtime::Execution::Operators::HJBuildSlicingVarSized>(
        operatorHandlerIndex,
        buildOperator->getInputSchema(),
        buildOperator->getJoinFieldName(),
        buildOperator->getBuildSide(),
        buildOperator->getInputSchema()->getSchemaSizeInBytes(),
        std::move(timeFunction),
        buildOperator->getJoinStrategy());
}

Runtime::Execution::Operators::ExecutableOperatorPtr LowerPhysicalToNautilusOperators::lowerHJBucketing(
    const std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator>& buildOperator,
    uint64_t operatorHandlerIndex,
    Runtime::Execution::Operators::TimeFunctionPtr timeFunction,
    uint64_t windowSize,
    uint64_t windowSlide) {
    return std::make_shared<Runtime::Execution::Operators::HJBuildBucketing>(
        operatorHandlerIndex,
        buildOperator->getInputSchema(),
        buildOperator->getJoinFieldName(),
        buildOperator->getBuildSide(),
        buildOperator->getInputSchema()->getSchemaSizeInBytes(),
        std::move(timeFunction),
        buildOperator->getJoinStrategy(),
        windowSize,
        windowSlide);
}

std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lowerWindowSinkOperator(Runtime::Execution::PhysicalOperatorPipeline& pipeline,
                                                          const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
                                                          std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto sinkOperator = physicalOperator->as<PhysicalOperators::PhysicalWindowSinkOperator>();
    if (sinkOperator->getWindowDefinition()->isKeyed()) {
        return lowerKeyedWindowSinkOperator(pipeline, physicalOperator, operatorHandlers);
    } else {
        return lowerNonKeyedWindowSinkOperator(pipeline, physicalOperator, operatorHandlers);
    }
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerKeyedWindowSinkOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalSWS = physicalOperator->as<PhysicalOperators::PhysicalWindowSinkOperator>();

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

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerNonKeyedWindowSinkOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalSWS = physicalOperator->as<PhysicalOperators::PhysicalWindowSinkOperator>();

    auto aggregations = physicalSWS->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    // We assume that the first field of the output schema is the window start ts, and the second field is the window end ts.
    // TODO this information should be stored in the logical window descriptor otherwise this assumption may fail in the future.
    auto startTs = physicalSWS->getOutputSchema()->get(0)->getName();
    auto endTs = physicalSWS->getOutputSchema()->get(1)->getName();
    auto windowType = physicalSWS->getWindowDefinition()->getWindowType();
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

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {

    auto sliceMerging = physicalOperator->as<PhysicalOperators::PhysicalSliceMergingOperator>();
    if (sliceMerging->getWindowDefinition()->isKeyed()) {
        return lowerKeyedSliceMergingOperator(pipeline, physicalOperator, operatorHandlers);
    } else {
        return lowerNonKeyedSliceMergingOperator(pipeline, physicalOperator, operatorHandlers);
    }
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerNonKeyedSliceMergingOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGSMO = physicalOperator->as<PhysicalOperators::PhysicalSliceMergingOperator>();
    auto handler = std::make_shared<Runtime::Execution::Operators::NonKeyedSliceMergingHandler>();
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
    if (isTumblingWindow || options->getWindowingStrategy() == WindowingStrategy::BUCKETING) {
        sliceMergingAction = std::make_unique<Runtime::Execution::Operators::NonKeyedWindowEmitAction>(
            aggregationFunctions,
            startTs,
            endTs,
            physicalGSMO->getWindowDefinition()->getOriginId());
    } else {
        const auto timeBasedWindowType =
            physicalGSMO->getWindowDefinition()->getWindowType()->as<Windowing::TimeBasedWindowType>();
        const auto& [windowSize, windowSlide, _] = Util::getWindowingParameters(*timeBasedWindowType);
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
    auto physicalGSMO = physicalOperator->as<PhysicalOperators::PhysicalSliceMergingOperator>();

    auto handler = std::make_shared<Runtime::Execution::Operators::KeyedSliceMergingHandler>();

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
    if (isTumblingWindow || options->getWindowingStrategy() == WindowingStrategy::BUCKETING) {
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
        const auto windowType = physicalGSMO->getWindowDefinition()->getWindowType()->as<Windowing::TimeBasedWindowType>();
        const auto& [windowSize, windowSlide, timeFunction] = Util::getWindowingParameters(*windowType);
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
        return std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
            timeStampField,
            timeWindow->getTimeCharacteristic()->getTimeUnit());
    }
    NES_THROW_RUNTIME_ERROR("Timefunction could not be created for the following window definition: " << timeWindow->toString());
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {

    auto physicalPreAggregation = physicalOperator->as<PhysicalOperators::PhysicalSlicePreAggregationOperator>();
    if (physicalPreAggregation->getWindowDefinition()->isKeyed()) {
        return lowerKeyedPreAggregationOperator(pipeline, physicalPreAggregation, operatorHandlers);
    } else {
        return lowerNonKeyedPreAggregationOperator(pipeline, physicalPreAggregation, operatorHandlers);
    }
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerNonKeyedPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGTLPAO = physicalOperator->as<PhysicalOperators::PhysicalSlicePreAggregationOperator>();
    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = physicalGTLPAO->getWindowDefinition()->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    auto timeWindow = windowDefinition->getWindowType()->as<Windowing::TimeBasedWindowType>();
    auto timeFunction = lowerTimeFunction(timeWindow);

    if (options->getWindowingStrategy() == WindowingStrategy::SLICING) {

        auto handler = std::make_shared<Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler>(
            timeWindow->getSize().getTime(),
            timeWindow->getSlide().getTime(),
            windowDefinition->getInputOriginIds());

        operatorHandlers.emplace_back(handler);
        auto slicePreAggregation =
            std::make_shared<Runtime::Execution::Operators::NonKeyedSlicePreAggregation>(operatorHandlers.size() - 1,
                                                                                         std::move(timeFunction),
                                                                                         aggregationFunctions);
        return slicePreAggregation;
    } else if (options->getWindowingStrategy() == WindowingStrategy::BUCKETING) {
        auto timeBasedWindowType = windowDefinition->getWindowType()->as<Windowing::TimeBasedWindowType>();

        auto handler = std::make_shared<Runtime::Execution::Operators::NonKeyedBucketPreAggregationHandler>(
            timeBasedWindowType->getSize().getTime(),
            timeBasedWindowType->getSlide().getTime(),
            windowDefinition->getInputOriginIds());
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
LowerPhysicalToNautilusOperators::lowerKeyedPreAggregationOperator(
    Runtime::Execution::PhysicalOperatorPipeline&,
    const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers) {
    auto physicalGTLPAO = physicalOperator->as<PhysicalOperators::PhysicalSlicePreAggregationOperator>();

    auto windowDefinition = physicalGTLPAO->getWindowDefinition();
    auto aggregations = windowDefinition->getWindowAggregation();
    auto aggregationFunctions = lowerAggregations(aggregations);
    auto timeWindow = windowDefinition->getWindowType()->as<Windowing::TimeBasedWindowType>();
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

    if (options->getWindowingStrategy() == WindowingStrategy::SLICING) {

        auto physicalPreAgg =
    physicalOperator->as<PhysicalOperators::PhysicalSlicePreAggregationOperator>();
        auto preAggHandler = physicalPreAgg->getPreAggregationHandler();
        // now push it into operatorHandlers
        operatorHandlers.push_back(preAggHandler);
        auto handlerIndex = operatorHandlers.size()-1;

        auto sliceMergingOperator = std::make_shared<Runtime::Execution::Operators::KeyedSlicePreAggregation>(
            handlerIndex,
            std::move(timeFunction),
            keyReadExpressions,
            keyDataTypes,
            aggregationFunctions,
            std::make_unique<Nautilus::Interface::MurMur3HashFunction>());
        return sliceMergingOperator;
    } else if (options->getWindowingStrategy() == WindowingStrategy::BUCKETING) {

        auto handler = std::make_shared<Runtime::Execution::Operators::KeyedBucketPreAggregationHandler>(
            timeWindow->getSize().getTime(),
            timeWindow->getSlide().getTime(),
            windowDefinition->getInputOriginIds());

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
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(fieldExpression,
                                                                               eventTimeWatermarkStrategy->getTimeUnit()));
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
    auto contentBasedWindowType =
        thresholdWindowOperator->getWindowDefinition()->getWindowType()->as<Windowing::ContentBasedWindowType>();
    auto thresholdWindowType = Windowing::ContentBasedWindowType::asThresholdWindow(contentBasedWindowType);
    NES_INFO("lowerThresholdWindow Predicate {}", thresholdWindowType->getPredicate()->toString());
    auto predicate = expressionProvider->lowerExpression(thresholdWindowType->getPredicate());
    auto minCount = thresholdWindowType->getMinimumCount();

    auto aggregations = thresholdWindowOperator->getWindowDefinition()->getWindowAggregation();
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
LowerPhysicalToNautilusOperators::lowerAggregations(const std::vector<Windowing::WindowAggregationDescriptorPtr>& aggs) {
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

LowerPhysicalToNautilusOperators::~LowerPhysicalToNautilusOperators() = default;

}// namespace NES::QueryCompilation
