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
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Functions/NodeFunction.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/NautilusOperatorLoweringPlugin.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Types/TimeBasedWindowType.hpp>

namespace NES::QueryCompilation
{
class FunctionProvider;

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
        const std::shared_ptr<Runtime::Execution::Operators::Operator>& parentOperator,
        const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
        size_t bufferSize,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    std::shared_ptr<Runtime::Execution::Operators::Operator>
    lowerScan(const PhysicalOperators::PhysicalOperatorPtr& physicalOperator, size_t bufferSize);

    static std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> lowerEmit(
        const PhysicalOperators::PhysicalOperatorPtr& physicalOperator,
        size_t bufferSize,
        std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);

    static std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
    lowerFilter(const PhysicalOperators::PhysicalOperatorPtr& physicalOperator);

    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
    lowerMap(const PhysicalOperators::PhysicalOperatorPtr& physicalOperator);

    std::shared_ptr<QueryCompilerOptions> options;
    std::unique_ptr<FunctionProvider> functionProvider;
};
}
