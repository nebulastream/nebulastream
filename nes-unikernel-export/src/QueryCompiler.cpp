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

#include <ExportPhaseFactory.h>
#include <OperatorHandlerTracer.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <QueryCompiler.hpp>
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <ranges>
#include <utility>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <NoOpSourceDescriptor.hpp>
#include <NoOpPhysicalSourceType.hpp>

template<typename TO, typename FROM>
std::unique_ptr<TO> static_unique_pointer_cast(std::unique_ptr<FROM> &&old) {
    assert(dynamic_cast<TO *>(old.get()));
    return std::unique_ptr<TO>{static_cast<TO *>(old.release())};
    // conversion: unique_ptr<FROM>->FROM*->TO*->unique_ptr<TO>
}

static std::map<NES::PipelineId, SinkStage>
getSinks(std::vector<NES::QueryCompilation::OperatorPipelinePtr> pipelines) {

    auto filterSink = std::ranges::views::filter([](NES::QueryCompilation::OperatorPipelinePtr pipeline) {
        return pipeline->isSinkPipeline();
    });

    std::map<NES::PipelineId, SinkStage> sinkMap;
    std::ranges::for_each(pipelines | filterSink, [&sinkMap](NES::QueryCompilation::OperatorPipelinePtr pipeline) {
        auto sinks =
                pipeline->getQueryPlan()->getOperatorByType<NES::QueryCompilation::PhysicalOperators::PhysicalSinkOperator>();
        NES_ASSERT2_FMT(sinks.size() == 1, "Expected a single source");
        auto sink = sinks[0];
        std::vector<NES::PipelineId> predecessor;
        std::ranges::transform(pipeline->getPredecessors(), std::back_inserter(predecessor), [](auto pred) {
            return pred->getPipelineId();
        });
        sinkMap[pipeline->getPipelineId()] = SinkStage{pipeline->getPipelineId(),
                                                       predecessor,
                                                       sink->getSinkDescriptor()->as<NES::Network::NetworkSinkDescriptor>()};
    });

    return sinkMap;
}

static std::map<NES::PipelineId, NES::SourceDescriptorPtr>
getSources(std::vector<NES::QueryCompilation::OperatorPipelinePtr> pipelines,
           NES::Catalogs::Source::SourceCatalogPtr catalog) {
    auto filterSource = std::ranges::views::filter([](NES::QueryCompilation::OperatorPipelinePtr pipeline) {
        return pipeline->isSourcePipeline();
    });

    std::map<NES::PipelineId, NES::SourceDescriptorPtr> sourceMap;
    std::ranges::for_each(pipelines | filterSource,
                          [&sourceMap, &catalog](NES::QueryCompilation::OperatorPipelinePtr pipeline) {
                              auto sources =
                                      pipeline->getQueryPlan()->getOperatorByType<NES::QueryCompilation::PhysicalOperators::PhysicalSourceOperator>();
                              NES_ASSERT2_FMT(sources.size() == 1, "Expected a single source");
                              auto source = sources[0];

                              if (source->getSourceDescriptor()->instanceOf<NES::Network::NetworkSourceDescriptor>()) {
                                  sourceMap[pipeline->getPipelineId()] = source->getSourceDescriptor();
                                  return;
                              }

                              auto physicalSourceType = catalog->getPhysicalSources(
                                      source->getSourceDescriptor()->getLogicalSourceName())[0]->getPhysicalSource()->getPhysicalSourceType();
                              auto noOpSource = std::dynamic_pointer_cast<NES::NoOpPhysicalSourceType>(
                                      physicalSourceType);
                              sourceMap[pipeline->getPipelineId()] = NES::NoOpSourceDescriptor::create(
                                      source->getInputSchema(), source->getSourceDescriptor()->getLogicalSourceName(),
                                      noOpSource->getTCP(), source->getOriginId(), source->getId());
                          });

    return sourceMap;
}

static std::pair<std::vector<Stage>,
        std::vector<NES::Runtime::Unikernel::OperatorHandlerDescriptor>>
getStages(std::vector<NES::QueryCompilation::OperatorPipelinePtr> pipelines,
          NES::QueryCompilation::QueryCompilerOptionsPtr queryCompilerOptions,
          size_t bufferSize) {
    auto lowerToNautilus = std::make_shared<NES::QueryCompilation::LowerPhysicalToNautilusOperators>(
            queryCompilerOptions);
    auto compilationPhase = std::make_shared<NES::QueryCompilation::NautilusCompilationPhase>(queryCompilerOptions);

    std::vector<StageWithHandlers> stagesWithHandlers;
    auto filter = std::ranges::views::filter([](NES::QueryCompilation::OperatorPipelinePtr pipeline) {
        return pipeline->isOperatorPipeline();
    });

    std::map<NES::Runtime::Execution::OperatorHandlerPtr, size_t> uniqueOperatorHandlers;
    std::ranges::transform(
            pipelines | filter,
            std::back_inserter(stagesWithHandlers),
            [&compilationPhase, &lowerToNautilus, bufferSize, &uniqueOperatorHandlers](
                    NES::QueryCompilation::OperatorPipelinePtr pipeline) -> StageWithHandlers {
                auto pipelineId = pipeline->getPipelineId();

                std::optional<NES::PipelineId> successor = std::nullopt;
                if (!pipeline->getSuccessors().empty()) {
                    successor.emplace(pipeline->getSuccessors()[0]->getPipelineId());
                }

                std::vector<NES::PipelineId> predecessor;
                std::ranges::transform(pipeline->getPredecessors(), std::back_inserter(predecessor), [](auto pred) {
                    return pred->getPipelineId();
                });

                auto stage1 = lowerToNautilus->apply(pipeline, bufferSize);
                std::vector<NES::Runtime::Unikernel::OperatorHandlerDescriptor> descriptors;
                auto handler = stage1->getQueryPlan()
                        ->getRootOperators()[0]
                        ->as<NES::QueryCompilation::NautilusPipelineOperator>()
                        ->getOperatorHandlers();
                std::ranges::for_each(handler, [&uniqueOperatorHandlers](auto handler) {
                    uniqueOperatorHandlers[handler]++;
                });

                auto stage = compilationPhase->getCompiledPipelineStage(stage1);
                auto nautilusStage =
                        static_unique_pointer_cast<NES::Runtime::Execution::CompiledExecutablePipelineStage>(
                                std::move(stage));
                return {pipelineId, successor, predecessor, std::move(nautilusStage), handler};
            });

    std::vector<Stage> result;
    SharedOperatorHandlerId sharedHandlerId = 0;
    std::map<NES::Runtime::Execution::OperatorHandlerPtr, SharedOperatorHandlerId> sharedHandlerByPointer;
    std::ranges::transform(stagesWithHandlers,
                           std::back_inserter(result),
                           [&uniqueOperatorHandlers, &sharedHandlerByPointer, &sharedHandlerId](auto &stage) -> Stage {
                               std::vector<EitherSharedOrLocal> handlers;
                               for (const auto &handler: stage.handlers) {
                                   if (sharedHandlerByPointer.contains(handler)) {
                                       handlers.emplace_back(sharedHandlerByPointer[handler]);
                                       continue;
                                   }

                                   if (uniqueOperatorHandlers[handler] > 1) {
                                       auto newSharedHandlerId = sharedHandlerId++;
                                       sharedHandlerByPointer[handler] = newSharedHandlerId;
                                       handlers.emplace_back(newSharedHandlerId);
                                       continue;
                                   }

                                   handlers.emplace_back(handler->getDescriptor());
                               }
                               return {std::move(stage.pipelineId),
                                       std::move(stage.successor),
                                       std::move(stage.predecessors),
                                       std::move(stage.stage),
                                       std::move(handlers)};
                           });

    std::vector<NES::Runtime::Unikernel::OperatorHandlerDescriptor> sharedHandler(sharedHandlerId);

    for (auto &[handler, id]: sharedHandlerByPointer) {
        sharedHandler[id] = handler->getDescriptor();
    }

    return std::make_pair(std::move(result), std::move(sharedHandler));
}

std::tuple<std::map<NES::PipelineId, SinkStage>,
        std::map<NES::PipelineId, NES::SourceDescriptorPtr>,
        std::vector<Stage>,
        std::vector<NES::Runtime::Unikernel::OperatorHandlerDescriptor>
>
ExportQueryCompiler::compile(NES::QueryPlanPtr queryPlan, size_t bufferSize,
                             NES::Catalogs::Source::SourceCatalogPtr sourceCatalog) {
    using namespace NES;
    auto phaseFactory = QueryCompilation::Phases::ExportPhaseFactory::create();
    QueryCompilation::QueryCompilerOptionsPtr queryCompilerOptions =
            QueryCompilation::QueryCompilerOptions::createDefaultOptions();
    queryCompilerOptions->setQueryCompiler(NES::QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER);
    queryCompilerOptions->setWindowingStrategy(NES::QueryCompilation::WindowingStrategy::SLICING);
    NES::Runtime::Unikernel::OperatorHandlerTracer::reset();
    auto lowerToPhysical = phaseFactory->createLowerLogicalQueryPlanPhase(queryCompilerOptions);
    auto pipeLinePhase = phaseFactory->createPipeliningPhase(queryCompilerOptions);
    auto scanAndEmitPhase = phaseFactory->createAddScanAndEmitPhase(queryCompilerOptions);

    NES_INFO("Query Plan: \n{}", queryPlan->toString());
    auto qp1 = lowerToPhysical->apply(queryPlan);
    auto qp2 = pipeLinePhase->apply(qp1);
    scanAndEmitPhase->apply(qp2);

    NES_INFO("After Pipelining:\n{}", qp2->toString());

    auto pipelines = qp2->getPipelines();
    auto [stages, sharedHandler] = getStages(pipelines, queryCompilerOptions, bufferSize);
    return {getSinks(pipelines), getSources(pipelines, sourceCatalog), std::move(stages), sharedHandler};
}
