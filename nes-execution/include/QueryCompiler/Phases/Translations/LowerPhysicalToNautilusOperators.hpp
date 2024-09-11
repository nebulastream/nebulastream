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
#pragma once

#include <cstddef>
#include <memory>
#include <vector>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Aggregation/AggregationValue.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Expressions/ExpressionNode.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/Streaming/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/NautilusOperatorLoweringPlugin.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Types/TimeBasedWindowType.hpp>

namespace NES::QueryCompilation
{
class ExpressionProvider;

/// This phase lowers a pipeline plan of physical operators into a pipeline plan of nautilus operators.
/// The lowering of individual operators is defined by the nautilus operator provider to improve extendability.
class LowerPhysicalToNautilusOperators : std::enable_shared_from_this<LowerPhysicalToNautilusOperators>
{
public:
    explicit LowerPhysicalToNautilusOperators(std::shared_ptr<QueryCompilerOptions> options);
    ~LowerPhysicalToNautilusOperators();

    /// Applies the phase on a pipelined query plan.
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr pipelinedQueryPlan, size_t bufferSize);
    /// Applies the phase on a pipelined and lower physical operator to generatable once.
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline, size_t bufferSize);

private:
    std::shared_ptr<Runtime::Execution::Operators::Operator> lower(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator,
        const PhysicalOperators::PhysicalOperatorPtr& operatorPtr,
        size_t bufferSize,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::Operator> lowerScan(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        size_t bufferSize);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> lowerEmit(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        size_t bufferSize);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
    lowerFilter(Runtime::Execution::PhysicalOperatorPipeline& pipeline, const PhysicalOperators::PhysicalOperatorPtr& physicalOperator);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> lowerLimit(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
    lowerMap(Runtime::Execution::PhysicalOperatorPipeline& pipeline, const PhysicalOperators::PhysicalOperatorPtr& physicalOperator);

    std::unique_ptr<Runtime::Execution::Operators::TimeFunction>
    lowerTimeFunction(const Windowing::TimeBasedWindowTypePtr& timeBasedWindowType);

    std::shared_ptr<Runtime::Execution::Operators::Operator> lowerSliceMergingOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::Operator> lowerNonKeyedSliceMergingOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::Operator> lowerKeyedSliceMergingOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> lowerPreAggregationOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> lowerNonKeyedPreAggregationOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> lowerKeyedPreAggregationOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::Operator> lowerWindowSinkOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::Operator> lowerNonKeyedWindowSinkOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);
    std::shared_ptr<Runtime::Execution::Operators::Operator> lowerKeyedWindowSinkOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);
    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> lowerWatermarkAssignmentOperator(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> lowerThresholdWindow(
        Runtime::Execution::PhysicalOperatorPipeline& pipeline,
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        uint64_t handlerIndex);

    std::vector<std::shared_ptr<Runtime::Execution::Aggregation::AggregationFunction>>
    lowerAggregations(const std::vector<Windowing::WindowAggregationDescriptorPtr>& functions);

    std::unique_ptr<Runtime::Execution::Aggregation::AggregationValue>
    getAggregationValueForThresholdWindow(Windowing::WindowAggregationDescriptor::Type aggregationType, DataTypePtr inputType);

    Runtime::Execution::Operators::ExecutableOperatorPtr lowerHJSlicing(
        const std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator>& hashJoinBuildOperator,
        uint64_t operatorHandlerIndex,
        Runtime::Execution::Operators::TimeFunctionPtr timeFunction);

    Runtime::Execution::Operators::ExecutableOperatorPtr lowerHJSlicingVarSized(
        const std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator>& hashJoinBuildOperator,
        uint64_t operatorHandlerIndex,
        Runtime::Execution::Operators::TimeFunctionPtr timeFunction);

    Runtime::Execution::Operators::ExecutableOperatorPtr lowerHJBucketing(
        const std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator>& hashJoinBuildOperator,
        uint64_t operatorHandlerIndex,
        Runtime::Execution::Operators::TimeFunctionPtr timeFunction,
        uint64_t windowSize,
        uint64_t windowSlide);

    Runtime::Execution::Operators::ExecutableOperatorPtr lowerNLJSlicing(
        std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator> nestedLoopJoinBuildOperator,
        uint64_t operatorHandlerIndex,
        Runtime::Execution::Operators::TimeFunctionPtr timeFunction);

    Runtime::Execution::Operators::ExecutableOperatorPtr lowerNLJBucketing(
        std::shared_ptr<PhysicalOperators::PhysicalStreamJoinBuildOperator> nestedLoopJoinBuildOperator,
        uint64_t operatorHandlerIndex,
        Runtime::Execution::Operators::TimeFunctionPtr timeFunction,
        uint64_t windowSize,
        uint64_t windowSlide);

    std::shared_ptr<QueryCompilerOptions> options;
    std::unique_ptr<ExpressionProvider> expressionProvider;
};
} /// namespace NES::QueryCompilation
