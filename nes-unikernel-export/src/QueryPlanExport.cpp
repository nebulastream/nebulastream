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

#include <NoOp/NoOpPhysicalSourceType.hpp>
#include <NoOp/NoOpSourceDescriptor.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryPlanExport.hpp>
namespace NES::Unikernel::Export {
std::unordered_map<PipelineId, QueryPlanExporter::ExportSinkDescriptor>
QueryPlanExporter::getSinks(const QueryPipeliner::Result& pipeliningResult) const {
    auto& pipelines = pipeliningResult.stages;
    auto filterSink = std::ranges::views::filter([](const Stage& stage) {
        return stage.pipeline->isSinkPipeline();
    });

    std::unordered_map<PipelineId, ExportSinkDescriptor> sinkMap;
    std::ranges::for_each(pipelines | filterSink, [&sinkMap](const auto& stage) {
        auto& pipeline = stage.pipeline;
        auto sinks =
            pipeline->getQueryPlan()->template getOperatorByType<QueryCompilation::PhysicalOperators::PhysicalSinkOperator>();
        NES_ASSERT2_FMT(sinks.size() == 1, "Expected a single source");
        auto sink = sinks[0];
        std::vector<PipelineId> predecessor;
        std::ranges::transform(pipeline->getPredecessors(), std::back_inserter(predecessor), [](auto pred) {
            return pred->getPipelineId();
        });
        sinkMap[pipeline->getPipelineId()] =
            ExportSinkDescriptor{predecessor, sink->getSinkDescriptor()->template as<Network::NetworkSinkDescriptor>()};
    });

    return sinkMap;
}
std::unordered_map<PipelineId, QueryPlanExporter::ExportSourceDescriptor>
QueryPlanExporter::getSources(const QueryPipeliner::Result& pipeliningResult) const {
    auto& pipelines = pipeliningResult.stages;
    auto filterSource = std::ranges::views::filter([](const Stage& stage) {
        return stage.pipeline->isSourcePipeline();
    });

    std::unordered_map<PipelineId, ExportSourceDescriptor> sourceMap;
    std::ranges::for_each(pipelines | filterSource, [&sourceMap, this](const Stage& sourceStage) {
        auto& pipeline = sourceStage.pipeline;
        auto sources = pipeline->getQueryPlan()->getOperatorByType<QueryCompilation::PhysicalOperators::PhysicalSourceOperator>();
        NES_ASSERT2_FMT(sources.size() == 1, "Expected a single source");
        auto source = sources[0];

        if (source->getSourceDescriptor()->instanceOf<Network::NetworkSourceDescriptor>()) {
            sourceMap[pipeline->getPipelineId()] = {source->getSourceDescriptor(), source->getOriginId()};
            return;
        }

        auto physicalSourceType = sourceCatalog->getPhysicalSources(source->getSourceDescriptor()->getLogicalSourceName())[0]
                                      ->getPhysicalSource()
                                      ->getPhysicalSourceType();
        auto noOpSource = std::dynamic_pointer_cast<NoOpPhysicalSourceType>(physicalSourceType);
        sourceMap[pipeline->getPipelineId()] = {
            NoOpSourceDescriptor::create(source->getInputSchema(),
                                         noOpSource->getSchemaType(),
                                         source->getSourceDescriptor()->getLogicalSourceName(),
                                         noOpSource->getTCP(),
                                         source->getId()),
            source->getOriginId()};
    });

    return sourceMap;
}
QueryPlanExporter::Result QueryPlanExporter::exportQueryPlan(const QueryPipeliner::Result& result) const {
    return {getSources(result), getSinks(result)};
}
}// namespace NES::Unikernel::Export