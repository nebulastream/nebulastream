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
#include <Operators/LogicalOperators/LogicalBroadcastOperator.hpp>
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalLimitOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/LogicalOpenCLOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalProjectionOperator.hpp>
#include <Operators/LogicalOperators/LogicalRenameSourceOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Operators/LogicalOperators/Sinks/LogicalSinkOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceOperator.hpp>
#include <Operators/LogicalOperators/UDFs/FlatMapUDF/LogicalFlatMapUDFOperator.hpp>
#include <Operators/LogicalOperators/UDFs/MapUDF/LogicalMapUDFOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/LogicalWatermarkAssignerOperator.hpp>
#include <Operators/LogicalOperators/Windows/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowOperator.hpp>
#include <Operators/LogicalOperators/Windows/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windows/SliceMergingOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowComputationOperator.hpp>

namespace NES {

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createSourceOperator(const SourceDescriptorPtr& sourceDescriptor, OperatorId id, OriginId originId) {
    return std::make_shared<LogicalSourceOperator>(sourceDescriptor, id, originId);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createSinkOperator(const SinkDescriptorPtr& sinkDescriptor, OperatorId id) {
    return std::make_shared<LogicalSinkOperator>(sinkDescriptor, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createFilterOperator(const ExpressionNodePtr& predicate, OperatorId id) {
    return std::make_shared<LogicalFilterOperator>(predicate, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createRenameSourceOperator(const std::string& newSourceName, OperatorId id) {
    return std::make_shared<LogicalRenameSourceOperator>(newSourceName, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createLimitOperator(const uint64_t limit, OperatorId id) {
    return std::make_shared<LogicalLimitOperator>(limit, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createProjectionOperator(const std::vector<ExpressionNodePtr>& expressions,
                                                                             OperatorId id) {
    return std::make_shared<LogicalProjectionOperator>(expressions, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createMapOperator(const FieldAssignmentExpressionNodePtr& mapExpression,
                                                                      OperatorId id) {
    return std::make_shared<LogicalMapOperator>(mapExpression, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createInferModelOperator(std::string model,
                                                                             std::vector<ExpressionNodePtr> inputFieldsPtr,
                                                                             std::vector<ExpressionNodePtr> outputFieldsPtr,
                                                                             OperatorId id) {

    return std::make_shared<NES::InferModel::LogicalInferModelOperator>(model, inputFieldsPtr, outputFieldsPtr, id);
}

LogicalBinaryOperatorNodePtr LogicalOperatorFactory::createUnionOperator(OperatorId id) {
    return std::make_shared<LogicalUnionOperator>(id);
}

LogicalBinaryOperatorNodePtr LogicalOperatorFactory::createJoinOperator(const Join::LogicalJoinDefinitionPtr& joinDefinition,
                                                                        OperatorId id) {
    return std::make_shared<LogicalJoinOperator>(joinDefinition, id);
}

LogicalBinaryOperatorNodePtr
LogicalOperatorFactory::createBatchJoinOperator(const Join::Experimental::LogicalBatchJoinDefinitionPtr& batchJoinDefinition,
                                                OperatorId id) {
    return std::make_shared<Experimental::LogicalBatchJoinOperator>(batchJoinDefinition, id);
}

BroadcastLogicalOperatorNodePtr LogicalOperatorFactory::createBroadcastOperator(OperatorId id) {
    return std::make_shared<LogicalBroadcastOperator>(id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createWindowOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition, OperatorId id) {
    return std::make_shared<LogicalWindowOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createCentralWindowSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                               OperatorId id) {
    return std::make_shared<CentralWindowOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createSliceCreationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                               OperatorId id) {
    return std::make_shared<SliceCreationOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createWindowComputationSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                                   OperatorId id) {
    return std::make_shared<WindowComputationOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createSliceMergingSpecializedOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
                                                              OperatorId id) {
    return std::make_shared<SliceMergingOperator>(windowDefinition, id);
}

LogicalUnaryOperatorNodePtr LogicalOperatorFactory::createWatermarkAssignerOperator(
    const Windowing::WatermarkStrategyDescriptorPtr& watermarkStrategyDescriptor,
    OperatorId id) {
    return std::make_shared<LogicalWatermarkAssignerOperator>(watermarkStrategyDescriptor, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor, OperatorId id) {
    return std::make_shared<LogicalMapUDFOperator>(udfDescriptor, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createFlatMapUDFLogicalOperator(const Catalogs::UDF::UDFDescriptorPtr udfDescriptor, OperatorId id) {
    return std::make_shared<LogicalFlatMapUDFOperator>(udfDescriptor, id);
}

LogicalUnaryOperatorNodePtr
LogicalOperatorFactory::createOpenCLLogicalOperator(const Catalogs::UDF::JavaUdfDescriptorPtr javaUdfDescriptor, OperatorId id) {
    return std::make_shared<LogicalOpenCLOperator>(javaUdfDescriptor, id);
}

}// namespace NES
