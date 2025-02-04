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

#include <Operators/LogicalOperators/LogicalBatchJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalIntervalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalIntervalJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOpenCLOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/ReorderBuffersLogicalOperator.hpp>
#include <Operators/LogicalOperators/RenameSourceOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/StatisticCollection/LogicalStatisticWindowOperator.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/FlatMapUDFLogicalOperator.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/MapUDFLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Util/Placement/PlacementConstants.hpp>

namespace NES {

LogicalUnaryOperatorPtr
LogicalOperatorFactory::createSourceOperator(const SourceDescriptorPtr& sourceDescriptor, OperatorId id, OriginId originId) {
    return std::make_shared<SourceLogicalOperator>(sourceDescriptor, id, originId);
}

LogicalUnaryOperatorPtr
LogicalOperatorFactory::createSinkOperator(const SinkDescriptorPtr& sinkDescriptor, WorkerId workerId, OperatorId id) {
    auto sinkOperator = std::make_shared<SinkLogicalOperator>(sinkDescriptor, id);
    if (workerId != INVALID_WORKER_NODE_ID) {
        sinkOperator->addProperty(Optimizer::PINNED_WORKER_ID, workerId);
    }
    return sinkOperator;
}

LogicalUnaryOperatorPtr
LogicalOperatorFactory::createReorderTuplesOperator(OperatorId id) {
    return std::make_shared<ReorderTupleBuffersLogicalOperator>(id);
}

LogicalUnaryOperatorPtr LogicalOperatorFactory::createFilterOperator(const ExpressionNodePtr& predicate, OperatorId id) {
    return std::make_shared<LogicalFilterOperator>(predicate, id);
}

LogicalUnaryOperatorPtr LogicalOperatorFactory::createRenameSourceOperator(const std::string& newSourceName, OperatorId id) {
    return std::make_shared<RenameSourceOperator>(newSourceName, id);
}

LogicalUnaryOperatorPtr LogicalOperatorFactory::createLimitOperator(const uint64_t limit, OperatorId id) {
    return std::make_shared<LogicalLimitOperator>(limit, id);
}

LogicalUnaryOperatorPtr LogicalOperatorFactory::createProjectionOperator(const std::vector<ExpressionNodePtr>& expressions,
                                                                         OperatorId id) {
    return std::make_shared<LogicalProjectionOperator>(expressions, id);
}

LogicalUnaryOperatorPtr LogicalOperatorFactory::createMapOperator(const FieldAssignmentExpressionNodePtr& mapExpression,
                                                                  OperatorId id) {
    return std::make_shared<LogicalMapOperator>(mapExpression, id);
}

LogicalUnaryOperatorPtr
LogicalOperatorFactory::createStatisticBuildOperator(const Windowing::WindowTypePtr& window,
                                                     const Statistic::WindowStatisticDescriptorPtr& statisticDescriptor,
                                                     const Statistic::StatisticMetricHash metricHash,
                                                     const Statistic::SendingPolicyPtr sendingPolicy,
                                                     const Statistic::TriggerConditionPtr triggerCondition,
                                                     OperatorId id) {
    return std::make_shared<Statistic::LogicalStatisticWindowOperator>(id,
                                                                       window,
                                                                       statisticDescriptor,
                                                                       metricHash,
                                                                       sendingPolicy,
                                                                       triggerCondition);
}

LogicalUnaryOperatorPtr LogicalOperatorFactory::createInferModelOperator(std::string model,
                                                                         std::vector<ExpressionNodePtr> inputFieldsPtr,
                                                                         std::vector<ExpressionNodePtr> outputFieldsPtr,
                                                                         OperatorId id) {

    return std::make_shared<NES::InferModel::LogicalInferModelOperator>(model, inputFieldsPtr, outputFieldsPtr, id);
}

LogicalBinaryOperatorPtr LogicalOperatorFactory::createUnionOperator(OperatorId id) {
    return std::make_shared<LogicalUnionOperator>(id);
}

LogicalBinaryOperatorPtr LogicalOperatorFactory::createJoinOperator(const Join::LogicalJoinDescriptorPtr& joinDefinition,
                                                                    OperatorId id) {
    return std::make_shared<LogicalJoinOperator>(joinDefinition, id);
}

LogicalBinaryOperatorPtr
LogicalOperatorFactory::createBatchJoinOperator(const Join::Experimental::LogicalBatchJoinDescriptorPtr& batchJoinDefinition,
                                                OperatorId id) {
    return std::make_shared<Experimental::LogicalBatchJoinOperator>(batchJoinDefinition, id);
}

LogicalBinaryOperatorPtr
LogicalOperatorFactory::createIntervalJoinOperator(const Join::LogicalIntervalJoinDescriptorPtr& intervalJoinDefinition,
                                                   OperatorId id) {
    return std::make_shared<LogicalIntervalJoinOperator>(intervalJoinDefinition, id);
}

LogicalUnaryOperatorPtr
LogicalOperatorFactory::createWindowOperator(const Windowing::LogicalWindowDescriptorPtr& windowDefinition, OperatorId id) {
    return std::make_shared<LogicalWindowOperator>(windowDefinition, id);
}

LogicalUnaryOperatorPtr LogicalOperatorFactory::createWatermarkAssignerOperator(
    const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor,
    OperatorId id) {
    return std::make_shared<WatermarkAssignerLogicalOperator>(watermarkStrategyDescriptor, id);
}

LogicalUnaryOperatorPtr LogicalOperatorFactory::createMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor,
                                                                            OperatorId id) {
    return std::make_shared<MapUDFLogicalOperator>(udfDescriptor, id);
}

LogicalUnaryOperatorPtr
LogicalOperatorFactory::createFlatMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor, OperatorId id) {
    return std::make_shared<FlatMapUDFLogicalOperator>(udfDescriptor, id);
}

LogicalUnaryOperatorPtr
LogicalOperatorFactory::createOpenCLLogicalOperator(const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor, OperatorId id) {
    return std::make_shared<LogicalOpenCLOperator>(javaUdfDescriptor, id);
}

}// namespace NES
