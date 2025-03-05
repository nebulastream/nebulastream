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
#include <cstdint>
#include <memory>
#include <numeric>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/DelayShuffle/DelayBuffer.hpp>
#include <Execution/Operators/DelayShuffle/ShuffleTuples.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/EmitOperatorHandler.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Map.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Selection.hpp>
#include <Execution/Operators/SliceCache/GatherSlicesOperator.hpp>
#include <Execution/Operators/SortTuples/SortTuplesInBuffer.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationBuild.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationBuildCache.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationProbe.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationProbeNoProbing.hpp>
#include <Execution/Operators/Streaming/Aggregation/WindowAggregationOperator.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJBuildCache.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbeNoProbing.hpp>
#include <Execution/Operators/Streaming/WindowOperatorTriggerProbe.hpp>
#include <Execution/Operators/Watermark/EventTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Watermark/IngestionTimeWatermarkAssignment.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <QueryCompiler/Configurations/Enums/CompilationStrategy.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalStreamJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalGatherSlicesOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSelectionOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalShuffleBufferOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalShuffleTuplesOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSortTuplesInBuffer.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWindowTrigger.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationBuild.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalAggregationProbe.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/FunctionProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::QueryCompilation
{
LowerPhysicalToNautilusOperators::LowerPhysicalToNautilusOperators(Configurations::QueryCompilerConfiguration queryCompilerConfig)
    : queryCompilerConfig(std::move(queryCompilerConfig)), functionProvider(std::make_unique<FunctionProvider>())
{
}

std::shared_ptr<PipelineQueryPlan>
LowerPhysicalToNautilusOperators::apply(std::shared_ptr<PipelineQueryPlan> pipelinedQueryPlan, const size_t bufferSize)
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

std::shared_ptr<OperatorPipeline>
LowerPhysicalToNautilusOperators::apply(std::shared_ptr<OperatorPipeline> operatorPipeline, const size_t bufferSize) const
{
    const auto decomposedQueryPlan = operatorPipeline->getDecomposedQueryPlan();
    auto nodes = PlanIterator(*decomposedQueryPlan).snapshot();
    auto pipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>> operatorHandlers;
    std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator;

    std::unordered_map<PipelineId, std::stringstream> pipelineIdToText;

    for (const auto& node : nodes)
    {
        /// The scan and emit phase already contain the schema changes of the physical project operator.
        if (NES::Util::instanceOf<PhysicalOperators::PhysicalProjectOperator>(node))
        {
            continue;
        }
        NES_INFO("Lowering node: {}", *node);
        /// Adding the node and the pipeline id to the pipelineIdToText
        if (queryCompilerConfig.pipelinesTxtFilePath.getValue().empty())
        {
            continue;
        }
        if (not pipelineIdToText.contains(operatorPipeline->getPipelineId()))
        {
            pipelineIdToText[operatorPipeline->getPipelineId()] << "";
        }
        pipelineIdToText[operatorPipeline->getPipelineId()] << " Node: " << *NES::Util::as<PhysicalOperators::PhysicalOperator>(node)
                                                            << "\n";


        parentOperator
            = lower(*pipeline, parentOperator, NES::Util::as<PhysicalOperators::PhysicalOperator>(node), bufferSize, operatorHandlers);
    }

    if (not queryCompilerConfig.pipelinesTxtFilePath.getValue().empty())
    {
        std::ofstream file(queryCompilerConfig.pipelinesTxtFilePath.getValue(), std::ios::app);
        for (const auto& [pipelineId, text] : pipelineIdToText)
        {
            file << "Pipeline: " << pipelineId << "\n";
            file << text.str();
        }
    }


    const auto& rootOperators = decomposedQueryPlan->getRootOperators();
    for (const auto& root : rootOperators)
    {
        decomposedQueryPlan->removeAsRootOperator(root->getId());
    }
    const auto nautilusPipelineWrapper = std::make_shared<NautilusPipelineOperator>(getNextOperatorId(), pipeline, operatorHandlers);
    decomposedQueryPlan->addRootOperator(nautilusPipelineWrapper);
    return operatorPipeline;
}

std::shared_ptr<Runtime::Execution::Operators::Operator> LowerPhysicalToNautilusOperators::lower(
    Runtime::Execution::PhysicalOperatorPipeline& pipeline,
    const std::shared_ptr<Runtime::Execution::Operators::Operator>& parentOperator,
    const std::shared_ptr<PhysicalOperators::PhysicalOperator>& operatorNode,
    const size_t bufferSize,
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>>& operatorHandlers) const
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
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalAggregationBuild>(operatorNode))
    {
        const auto buildOperator = NES::Util::as<PhysicalOperators::PhysicalAggregationBuild>(operatorNode);
        const auto windowDefinition = buildOperator->getWindowDefinition();

        operatorHandlers.push_back(buildOperator->getOperatorHandler());
        const auto handlerIndex = operatorHandlers.size() - 1;
        auto timeFunction = buildOperator->getTimeFunction();
        auto aggregationFunctions = buildOperator->getAggregationFunctions(queryCompilerConfig);
        const auto valueSize = std::accumulate(
            aggregationFunctions.begin(),
            aggregationFunctions.end(),
            0,
            [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

        /// Lowering the key functions
        std::vector<std::unique_ptr<Runtime::Execution::Functions::Function>> keyFunctions;
        uint64_t keySize = 0;
        auto keyFunctionLogical = windowDefinition->getKeys();
        for (const auto& nodeFunctionKey : keyFunctionLogical)
        {
            const DefaultPhysicalTypeFactory typeFactory;
            const auto loweredFunctionType = nodeFunctionKey->getStamp();
            keyFunctions.emplace_back(FunctionProvider::lowerFunction(nodeFunctionKey));
            keySize += typeFactory.getPhysicalType(loweredFunctionType)->size();
        }
        const auto entrySize = sizeof(Nautilus::Interface::ChainedHashMapEntry) + keySize + valueSize;
        const auto entriesPerPage = queryCompilerConfig.pageSize.getValue() / entrySize;
        const auto& [fieldKeyNames, fieldValueNames] = buildOperator->getKeyAndValueFields();
        const auto& [fieldKeys, fieldValues] = Nautilus::Interface::MemoryProvider::ChainedEntryMemoryProvider::createFieldOffsets(
            *buildOperator->getInputSchema(), fieldKeyNames, fieldValueNames);

        Runtime::Execution::Operators::WindowAggregationOperator windowAggregationOperator(
            std::move(aggregationFunctions),
            std::make_unique<Nautilus::Interface::MurMur3HashFunction>(),
            fieldKeys,
            fieldValues,
            entriesPerPage,
            entrySize);
        const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction
            = std::make_unique<Nautilus::Interface::MurMur3HashFunction>();
        if (queryCompilerConfig.sliceCacheType == Configurations::SliceCacheType::NONE)
        {
            const auto executableAggregationBuild = std::make_shared<Runtime::Execution::Operators::AggregationBuild>(
                handlerIndex, std::move(timeFunction), std::move(keyFunctions), std::move(windowAggregationOperator));
            parentOperator->setChild(executableAggregationBuild);
            return executableAggregationBuild;
        }
        else
        {
            const auto executableAggregationBuild = std::make_shared<Runtime::Execution::Operators::AggregationBuildCache>(
                handlerIndex,
                std::move(timeFunction),
                std::move(keyFunctions),
                std::move(windowAggregationOperator),
                Configurations::SliceCacheOptions{
                    queryCompilerConfig.sliceCacheType.getValue(), queryCompilerConfig.numberOfEntriesSliceCache.getValue()});
            parentOperator->setChild(executableAggregationBuild);
            return executableAggregationBuild;
        }
    }
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalShuffleBufferOperator>(operatorNode))
    {
        auto shuffleBufferOperator = NES::Util::as<PhysicalOperators::PhysicalShuffleBufferOperator>(operatorNode);
        operatorHandlers.push_back(std::make_unique<Runtime::Execution::Operators::DelayBufferOperatorHandler>(
            shuffleBufferOperator->getUnorderedness(), shuffleBufferOperator->getMinDelay(), shuffleBufferOperator->getMaxDelay()));
        const auto shuffleBuffers = std::make_shared<Runtime::Execution::Operators::DelayBuffer>(operatorHandlers.size() - 1);
        parentOperator->setChild(shuffleBuffers);
        return shuffleBuffers;
    }
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalShuffleTuplesOperator>(operatorNode))
    {
        /// pass buffer size here
        auto shuffleTuplesOperator = NES::Util::as<PhysicalOperators::PhysicalShuffleTuplesOperator>(operatorNode);
        INVARIANT(
            shuffleTuplesOperator->getInputSchema()->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT,
            "Currently only row layout is supported");
        auto layout = std::make_shared<Memory::MemoryLayouts::RowLayout>(shuffleTuplesOperator->getInputSchema(), bufferSize);
        std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider
            = std::make_unique<Nautilus::Interface::MemoryProvider::RowTupleBufferMemoryProvider>(layout);


        operatorHandlers.push_back(
            std::make_unique<Runtime::Execution::Operators::ShuffleTuplesOperatorHandler>(shuffleTuplesOperator->getUnorderedness()));
        const auto shuffleTuplesExec
            = std::make_shared<Runtime::Execution::Operators::ShuffleTuples>(std::move(memoryProvider), operatorHandlers.size() - 1);
        pipeline.setRootOperator(shuffleTuplesExec);
        return shuffleTuplesExec;
    }
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalSortTuplesInBuffer>(operatorNode))
    {
        const auto sortTuplesInBuffer = NES::Util::as<PhysicalOperators::PhysicalSortTuplesInBuffer>(operatorNode);
        const auto projections = sortTuplesInBuffer->getOutputSchema()->getFieldNames();
        const auto sortField = sortTuplesInBuffer->getSortField();
        const auto memoryProviderInput = sortTuplesInBuffer->getMemoryProviderInput(bufferSize);
        const auto sortTuples
            = std::make_shared<Runtime::Execution::Operators::SortTuplesInBuffer>(memoryProviderInput, projections, sortField);
        pipeline.setRootOperator(sortTuples);
        return sortTuples;
    }
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalGatherSlicesOperator>(operatorNode))
    {
        const auto gatherSlicesOperator = NES::Util::as<PhysicalOperators::PhysicalGatherSlicesOperator>(operatorNode);
        auto timeFunction = gatherSlicesOperator->getTimeFunction().toTimeFunction();
        const auto windowSize = gatherSlicesOperator->getWindowSize();
        const auto windowSlide = gatherSlicesOperator->getWindowSlide();
        operatorHandlers.emplace_back(gatherSlicesOperator->getOperatorHandler());
        const auto gatherSlices = std::make_shared<Runtime::Execution::Operators::GatherSlicesOperator>(
            operatorHandlers.size() - 1, std::move(timeFunction), windowSize, windowSlide);
        parentOperator->setChild(gatherSlices);
        return gatherSlices;
    }
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalAggregationProbe>(operatorNode))
    {
        const auto probeOperator = NES::Util::as<PhysicalOperators::PhysicalAggregationProbe>(operatorNode);
        const auto windowDefinition = probeOperator->getWindowDefinition();
        const auto windowMetaData = probeOperator->getWindowMetaData();

        operatorHandlers.push_back(probeOperator->getOperatorHandler());
        const auto handlerIndex = operatorHandlers.size() - 1;
        auto aggregationFunctions = probeOperator->getAggregationFunctions(queryCompilerConfig);
        const auto valueSize = std::accumulate(
            aggregationFunctions.begin(),
            aggregationFunctions.end(),
            0,
            [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

        /// Lowering the key functions
        std::vector<std::unique_ptr<Runtime::Execution::Functions::Function>> keyFunctions;
        uint64_t keySize = 0;
        for (const auto& nodeFunctionKey : windowDefinition->getKeys())
        {
            const DefaultPhysicalTypeFactory typeFactory;
            const auto loweredFunctionType = nodeFunctionKey->getStamp();
            keyFunctions.emplace_back(FunctionProvider::lowerFunction(nodeFunctionKey));
            keySize += typeFactory.getPhysicalType(loweredFunctionType)->size();
        }
        const auto pageSize = queryCompilerConfig.pageSize.getValue();
        const auto numberOfBuckets = queryCompilerConfig.numberOfPartitions.getValue();
        const auto entrySize = sizeof(Nautilus::Interface::ChainedHashMapEntry) + keySize + valueSize;
        const auto entriesPerPage = pageSize / entrySize;
        const auto& [fieldKeyNames, fieldValueNames] = probeOperator->getKeyAndValueFields();
        const auto& [fieldKeys, fieldValues] = Nautilus::Interface::MemoryProvider::ChainedEntryMemoryProvider::createFieldOffsets(
            *probeOperator->getInputSchema(), fieldKeyNames, fieldValueNames);
        std::dynamic_pointer_cast<Runtime::Execution::Operators::AggregationOperatorHandler>(probeOperator->getOperatorHandler())
            ->setHashMapParams(keySize, valueSize, pageSize, numberOfBuckets);

        Runtime::Execution::Operators::WindowAggregationOperator windowAggregationOperator(
            std::move(aggregationFunctions),
            std::make_unique<Nautilus::Interface::MurMur3HashFunction>(),
            fieldKeys,
            fieldValues,
            entriesPerPage,
            entrySize);
        const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction
            = std::make_unique<Nautilus::Interface::MurMur3HashFunction>();

        /// Depending on the probe type, we have to create the correct aggregation probe
        switch (queryCompilerConfig.probeType)
        {
            case Configurations::ProbeType::PROBING: {
                const auto executableAggregationProbe = std::make_shared<Runtime::Execution::Operators::AggregationProbe>(
                    std::move(windowAggregationOperator), handlerIndex, windowMetaData);
                pipeline.setRootOperator(executableAggregationProbe);
                return executableAggregationProbe;
            }
            case Configurations::ProbeType::NO_PROBING: {
                const auto executableAggregationProbe = std::make_shared<Runtime::Execution::Operators::AggregationProbeNoProbing>(
                    std::move(windowAggregationOperator), handlerIndex, windowMetaData);
                pipeline.setRootOperator(executableAggregationProbe);
                return executableAggregationProbe;
            }
        }
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalWindowTrigger>(operatorNode))
    {
        auto triggerOperator = NES::Util::as<PhysicalOperators::PhysicalWindowTrigger>(operatorNode);
        const auto handlerIndex = operatorHandlers.size();
        operatorHandlers.push_back(triggerOperator->getOperatorHandler());
        const auto triggerOperatorNautilus = std::make_shared<Runtime::Execution::Operators::WindowOperatorTriggerProbe>(handlerIndex);
        pipeline.setRootOperator(triggerOperatorNautilus);
        return triggerOperatorNautilus;
    }
    else if (NES::Util::instanceOf<PhysicalOperators::PhysicalStreamJoinBuildOperator>(operatorNode))
    {
        const auto buildOperator = NES::Util::as<PhysicalOperators::PhysicalStreamJoinBuildOperator>(operatorNode);

        operatorHandlers.push_back(buildOperator->getJoinOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto memoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            queryCompilerConfig.pageSize.getValue(), buildOperator->getInputSchema());

        auto timeFunction = buildOperator->getTimeStampField().toTimeFunction();

        std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator> joinBuildNautilus;
        switch (buildOperator->getJoinStrategy())
        {
            case Configurations::StreamJoinStrategy::NESTED_LOOP_JOIN: {
                if (queryCompilerConfig.sliceCacheType == QueryCompilation::Configurations::SliceCacheType::NONE)
                {
                    joinBuildNautilus = std::make_shared<Runtime::Execution::Operators::NLJBuild>(
                        handlerIndex, buildOperator->getBuildSide(), std::move(timeFunction), memoryProvider);
                }
                else
                {
                    joinBuildNautilus = std::make_shared<Runtime::Execution::Operators::NLJBuildCache>(
                        handlerIndex,
                        buildOperator->getBuildSide(),
                        std::move(timeFunction),
                        memoryProvider,
                        Configurations::SliceCacheOptions{
                            queryCompilerConfig.sliceCacheType.getValue(), queryCompilerConfig.numberOfEntriesSliceCache.getValue()});
                }
                break;
            };
        }

        parentOperator->setChild(joinBuildNautilus);
        return joinBuildNautilus;
    }

    if (NES::Util::instanceOf<PhysicalOperators::PhysicalStreamJoinProbeOperator>(operatorNode))
    {
        const auto probeOperator = NES::Util::as<PhysicalOperators::PhysicalStreamJoinProbeOperator>(operatorNode);

        operatorHandlers.push_back(probeOperator->getJoinOperatorHandler());
        auto handlerIndex = operatorHandlers.size() - 1;

        auto leftMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            queryCompilerConfig.pageSize.getValue(), probeOperator->getLeftInputSchema());
        leftMemoryProvider->getMemoryLayout()->setKeyFieldNames({probeOperator->getJoinFieldNameLeft()});
        auto rightMemoryProvider = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(
            queryCompilerConfig.pageSize.getValue(), probeOperator->getRightInputSchema());
        rightMemoryProvider->getMemoryLayout()->setKeyFieldNames({probeOperator->getJoinFieldNameRight()});

        std::shared_ptr<Runtime::Execution::Operators::Operator> joinProbeNautilus;
        switch (probeOperator->getJoinStrategy())
        {
            case Configurations::StreamJoinStrategy::NESTED_LOOP_JOIN:
                switch (queryCompilerConfig.probeType)
                {
                    case Configurations::ProbeType::PROBING: {
                        joinProbeNautilus = std::make_shared<Runtime::Execution::Operators::NLJProbe>(
                            handlerIndex,
                            probeOperator->getJoinFunction(),
                            probeOperator->getWindowMetaData(),
                            probeOperator->getJoinSchema(),
                            leftMemoryProvider,
                            rightMemoryProvider);
                        break;
                    }
                    case Configurations::ProbeType::NO_PROBING: {
                        joinProbeNautilus = std::make_shared<Runtime::Execution::Operators::NLJProbeNoProbing>(
                            handlerIndex,
                            probeOperator->getJoinFunction(),
                            probeOperator->getWindowMetaData(),
                            probeOperator->getJoinSchema(),
                            leftMemoryProvider,
                            rightMemoryProvider);

                        break;
                    }
                }
                break;
        }

        pipeline.setRootOperator(joinProbeNautilus);
        return joinProbeNautilus;
    }
    if (NES::Util::instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>(operatorNode))
    {
        const auto watermarkAssignment = NES::Util::as<PhysicalOperators::PhysicalWatermarkAssignmentOperator>(operatorNode);
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
LowerPhysicalToNautilusOperators::lowerScan(const std::shared_ptr<PhysicalOperators::PhysicalOperator>& operatorNode, size_t bufferSize)
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
    const std::shared_ptr<PhysicalOperators::PhysicalOperator>& operatorNode,
    size_t bufferSize,
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>>& operatorHandlers)
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
LowerPhysicalToNautilusOperators::lowerFilter(const std::shared_ptr<PhysicalOperators::PhysicalOperator>& operatorPtr)
{
    const auto filterOperator = NES::Util::as<PhysicalOperators::PhysicalSelectionOperator>(operatorPtr);
    auto function = FunctionProvider::lowerFunction(filterOperator->getPredicate());
    return std::make_shared<Runtime::Execution::Operators::Selection>(std::move(function));
}

std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
LowerPhysicalToNautilusOperators::lowerMap(const std::shared_ptr<PhysicalOperators::PhysicalOperator>& operatorPtr)
{
    const auto mapOperator = NES::Util::as<PhysicalOperators::PhysicalMapOperator>(operatorPtr);
    const auto assignmentField = mapOperator->getMapFunction()->getField();
    const auto assignmentFunction = mapOperator->getMapFunction()->getAssignment();
    auto function = FunctionProvider::lowerFunction(assignmentFunction);
    return std::make_shared<Runtime::Execution::Operators::Map>(assignmentField->getFieldName(), std::move(function));
}

LowerPhysicalToNautilusOperators::~LowerPhysicalToNautilusOperators() = default;
}