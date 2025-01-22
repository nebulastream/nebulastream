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

#include <memory>
#include <API/Schema.hpp>
#include <Configurations/Enums/CompilationStrategy.hpp>
#include <Execution/Functions/ExecutableFunctionWriteField.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/EmitOperatorHandler.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Map.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Selection.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Execution/Operators/Watermark/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Watermark/IngestionTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

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
    auto nodes = PlanIterator(*decomposedQueryPlan).snapshot();
    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;
    std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator;

    for (const auto& node : nodes)
    {
        /// The scan and emit phase already contain the schema changes of the physical project operator.
        if (NES::Util::instanceOf<PhysicalOperators::PhysicalProjectOperator>(node))
        {
            continue;
        }
        NES_INFO("Lowering node: {}", *node);
        parentOperator
            = lower(*pipeline, parentOperator, NES::Util::as<PhysicalOperators::PhysicalOperator>(node), bufferSize, operatorHandlers);
    }
    const auto& rootOperators = decomposedQueryPlan->getRootOperators();
    for (const auto& root : rootOperators)
    {
        decomposedQueryPlan->removeAsRootOperator(root->getId());
    }
    auto nautilusPipelineWrapper = std::make_shared<NautilusPipelineOperator>(getNextOperatorId(), pipeline, operatorHandlers);
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
    NES_INFO("Lower node:{} to NautilusOperator.", *operatorNode);
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalScanOperator>(operatorNode))
    {
        auto scan = lowerScan(operatorNode, bufferSize);
        pipeline.setRootOperator(scan);
        return scan;
    }
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalEmitOperator>(operatorNode))
    {
        auto emit = lowerEmit(operatorNode, bufferSize, operatorHandlers);
        parentOperator->setChild(emit);
        return emit;
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalSelectionOperator>(operatorNode))
    {
        auto filter = lowerFilter(operatorNode);
        parentOperator->setChild(filter);
        return filter;
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalMapOperator>(operatorNode))
    {
        auto map = lowerMap(operatorNode);
        parentOperator->setChild(map);
        return map;
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalStreamJoinBuildOperator>(operatorNode))
    {
        auto buildOperator = NES::Util::as<PhysicalOperators::PhysicalStreamJoinBuildOperator>(operatorNode);

        operatorHandlers.push_back(buildOperator->getJoinOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto memoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            options->joinOptions.pageSize, buildOperator->getInputSchema());

        auto timeFunction = buildOperator->getTimeStampField().toTimeFunction();

        std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> joinBuildNautilus;
        switch (buildOperator->getJoinStrategy())
        {
            case StreamJoinStrategy::NESTED_LOOP_JOIN: {
                joinBuildNautilus = std::make_shared<Runtime::Execution::Operators::NLJBuild>(
                    handlerIndex, buildOperator->getBuildSide(), std::move(timeFunction), memoryProvider);
                break;
            };
        }

        parentOperator->setChild(joinBuildNautilus);
        return joinBuildNautilus;
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalStreamJoinProbeOperator>(operatorNode))
    {
        const auto probeOperator = NES::Util::as<PhysicalOperators::PhysicalStreamJoinProbeOperator>(operatorNode);

        operatorHandlers.push_back(probeOperator->getJoinOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            options->joinOptions.pageSize, probeOperator->getLeftInputSchema());
        leftMemoryProvider->getMemoryLayoutPtr()->setKeyFieldNames({probeOperator->getJoinFieldNameLeft()});
        auto rightMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            options->joinOptions.pageSize, probeOperator->getRightInputSchema());
        rightMemoryProvider->getMemoryLayoutPtr()->setKeyFieldNames({probeOperator->getJoinFieldNameRight()});

        std::shared_ptr<Runtime::Execution::Operators::Operator> joinProbeNautilus;
        switch (probeOperator->getJoinStrategy())
        {
            case StreamJoinStrategy::NESTED_LOOP_JOIN:
                joinProbeNautilus = std::make_shared<Runtime::Execution::Operators::NLJProbe>(
                    handlerIndex,
                    probeOperator->getJoinFunction(),
                    probeOperator->getWindowMetaData(),
                    probeOperator->getJoinSchema(),
                    leftMemoryProvider,
                    rightMemoryProvider);
                break;
        }

        pipeline.setRootOperator(joinProbeNautilus);
        return joinProbeNautilus;
    }
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>(operatorNode))
    {
        auto watermarkAssignment = NES::Util::as<PhysicalOperators::PhysicalWatermarkAssignmentOperator>(operatorNode);
        const auto watermarkDescriptor = watermarkAssignment->getWatermarkStrategyDescriptor();

        if (NES::Util::instanceOf<Windowing::IngestionTimeWatermarkStrategyDescriptor>(watermarkDescriptor))
        {
            const auto watermarkAssignmentOperator = std::make_shared<Runtime::Execution::Operators::IngestionTimeWatermarkAssignment>(
                std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>());
            parentOperator->setChild(watermarkAssignmentOperator);
            return watermarkAssignmentOperator;
        }
        if (NES::Util::instanceOf<Windowing::EventTimeWatermarkStrategyDescriptor>(watermarkDescriptor))
        {
            const auto eventTimeDescriptor = NES::Util::as<Windowing::EventTimeWatermarkStrategyDescriptor>(watermarkDescriptor);
            auto watermarkFieldFunction = FunctionProvider::lowerFunction(eventTimeDescriptor->getOnField());
            const auto watermarkAssignmentOperator = std::make_shared<Runtime::Execution::Operators::EventTimeWatermarkAssignment>(
                std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
                    std::move(watermarkFieldFunction), eventTimeDescriptor->getTimeUnit()));
            parentOperator->setChild(watermarkAssignmentOperator);
            return watermarkAssignmentOperator;
        }
    }

    throw UnknownPhysicalOperator(fmt::format("Cannot lower {}", *operatorNode));
}

std::shared_ptr<Runtime::Execution::Operators::Operator>
LowerPhysicalToNautilusOperators::lowerScan(const PhysicalOperators::PhysicalOperatorPtr& operatorNode, size_t bufferSize)
{
    auto schema = operatorNode->getOutputSchema();
    INVARIANT(
        schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT,
        "Currently only row layout is supported, current layout={}",
        magic_enum::enum_name(schema->getLayoutType()));
    /// pass buffer size here
    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
        = std::make_unique<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);
    return std::make_shared<Runtime::Execution::Operators::Scan>(std::move(memoryProvider), schema->getFieldNames());
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> LowerPhysicalToNautilusOperators::lowerEmit(
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode,
    size_t bufferSize,
    std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers)
{
    auto schema = operatorNode->getOutputSchema();
    INVARIANT(
        schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT,
        "Currently only row layout is supported, current layout={}",
        magic_enum::enum_name(schema->getLayoutType()));
    /// pass buffer size here
    auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(schema, bufferSize);
    std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
        = std::make_unique<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);
    operatorHandlers.push_back(std::make_unique<Runtime::Execution::Operators::EmitOperatorHandler>());
    return std::make_shared<Runtime::Execution::Operators::Emit>(operatorHandlers.size() - 1, std::move(memoryProvider));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerFilter(const PhysicalOperators::PhysicalOperatorPtr& operatorPtr)
{
    auto filterOperator = NES::Util::as<PhysicalOperators::PhysicalSelectionOperator>(operatorPtr);
    auto function = FunctionProvider::lowerFunction(filterOperator->getPredicate());
    return std::make_shared<Runtime::Execution::Operators::Selection>(std::move(function));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerMap(const PhysicalOperators::PhysicalOperatorPtr& operatorPtr)
{
    auto mapOperator = NES::Util::as<PhysicalOperators::PhysicalMapOperator>(operatorPtr);
    auto assignmentField = mapOperator->getMapFunction()->getField();
    auto assignmentFunction = mapOperator->getMapFunction()->getAssignment();
    auto function = FunctionProvider::lowerFunction(assignmentFunction);
    auto writeField = std::make_unique<Runtime::Execution::Functions::ExecutableFunctionWriteField>(
        assignmentField->getFieldName(), std::move(function));
    return std::make_shared<Runtime::Execution::Operators::Map>(std::move(writeField));
}

LowerPhysicalToNautilusOperators::~LowerPhysicalToNautilusOperators() = default;

}
