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
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Functions/ExecutableFunctionWriteField.hpp>
#include <Execution/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Map.hpp>
#include <Execution/Operators/Streaming/Selection.hpp>
#include <Execution/Operators/Streaming/Watermark/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/Watermark/IngestionTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Streaming/Watermark/TimeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Functions/NodeFunctionFieldAssignment.hpp>
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
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
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
    : options(std::move(options)), functionProvider(std::make_unique<FunctionProvider>())
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
        parentOperator = lower(*pipeline, parentOperator, node->as<PhysicalOperators::PhysicalOperator>(), bufferSize, operatorHandlers);
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
    if (operatorNode->instanceOf<PhysicalOperators::PhysicalScanOperator>())
    {
        auto scan = lowerScan(pipeline, operatorNode, bufferSize);
        pipeline.setRootOperator(scan);
        return scan;
    }
    else if (operatorNode->instanceOf<PhysicalOperators::PhysicalEmitOperator>())
    {
        auto emit = lowerEmit(pipeline, operatorNode, bufferSize);
        parentOperator->setChild(emit);
        return emit;
    }
    else if (operatorNode->instanceOf<PhysicalOperators::PhysicalFilterOperator>())
    {
        auto filter = lowerFilter(pipeline, operatorNode);
        parentOperator->setChild(filter);
        return filter;
    }
    else if (operatorNode->instanceOf<PhysicalOperators::PhysicalMapOperator>())
    {
        auto map = lowerMap(pipeline, operatorNode);
        parentOperator->setChild(map);
        return map;
    }

    throw UnknownPhysicalOperator();
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lowerScan(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorNode, size_t bufferSize)
{
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only row layout is supported");
    /// pass buffer size here
    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Runtime::Execution::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
        = std::make_unique<Runtime::Execution::MemoryProvider::RowTupleBufferMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerEmit(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorNode, size_t bufferSize)
{
    auto schema = operatorNode->getOutputSchema();
    NES_ASSERT(schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT, "Currently only row layout is supported");
    /// pass buffer size here
    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Runtime::Execution::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
        = std::make_unique<Runtime::Execution::MemoryProvider::RowTupleBufferMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Emit>(std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerFilter(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorPtr)
{
    auto filterOperator = operatorPtr->as<PhysicalOperators::PhysicalFilterOperator>();
    auto function = FunctionProvider::lowerFunction(filterOperator->getPredicate());
    return std::make_shared<Runtime::Execution::Operators::Selection>(std::move(function));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerMap(
    Runtime::Execution::PhysicalOperatorPipeline&, const PhysicalOperators::PhysicalOperatorPtr& operatorPtr)
{
    auto mapOperator = operatorPtr->as<PhysicalOperators::PhysicalMapOperator>();
    auto assignmentField = mapOperator->getMapFunction()->getField();
    auto assignmentFunction = mapOperator->getMapFunction()->getAssignment();
    auto function = FunctionProvider::lowerFunction(assignmentFunction);
    auto writeField = std::make_unique<Runtime::Execution::Functions::ExecutableFunctionWriteField>(
        assignmentField->getFieldName(), std::move(function));
    return std::make_shared<Runtime::Execution::Operators::Map>(std::move(writeField));
}

LowerPhysicalToNautilusOperators::~LowerPhysicalToNautilusOperators() = default;

}
