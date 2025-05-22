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

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Overloaded.hpp>
#include <CompiledQueryPlan.hpp>
#include <ExecutablePipelineStage.hpp>
#include <ExecutableQueryPlan.hpp>

namespace NES::Runtime
{

std::ostream& operator<<(std::ostream& os, const ExecutableQueryPlan& instantiatedQueryPlan)
{
    std::function<void(const std::weak_ptr<Execution::ExecutablePipeline>&, size_t)> printNode
        = [&os, &printNode](const std::weak_ptr<Execution::ExecutablePipeline>& weakPipeline, size_t indent)
    {
        auto pipeline = weakPipeline.lock();
        os << std::string(indent * 4, ' ') << *pipeline->stage << "(" << pipeline->id << ")" << '\n';
        for (const auto& successor : pipeline->successors)
        {
            printNode(successor, indent + 1);
        }
    };

    for (const auto& [source, successors] : instantiatedQueryPlan.sources)
    {
        os << *source << '\n';
        for (const auto& successor : successors)
        {
            printNode(successor, 1);
        }
    }
    return os;
}

std::unique_ptr<ExecutableQueryPlan> ExecutableQueryPlan::instantiate(
    Execution::CompiledQueryPlan& compiledQueryPlan,
    const std::shared_ptr<Memory::AbstractPoolProvider>& poolProvider,
    int numberOfBuffersInSourceLocalBufferPool)
{
    std::vector<SourceWithSuccessor> instantiatedSources;

    std::unordered_map<OriginId, std::vector<std::shared_ptr<Execution::ExecutablePipeline>>> instantiatedSinksWithSourcePredecessor;
    PipelineId::Underlying pipelineIdGenerator = compiledQueryPlan.pipelines.size() + PipelineId::INITIAL;

    for (auto& [descriptor, predecessors] : compiledQueryPlan.sinks)
    {
        auto sink = Execution::ExecutablePipeline::create(PipelineId(pipelineIdGenerator++), Sinks::SinkProvider::lower(*descriptor), {});
        compiledQueryPlan.pipelines.push_back(sink);
        for (const auto& predecessor : predecessors)
        {
            std::visit(
                Overloaded{
                    [&](const OriginId& source) { instantiatedSinksWithSourcePredecessor[source].push_back(sink); },
                    [&](const std::weak_ptr<Execution::ExecutablePipeline>& pipeline) { pipeline.lock()->successors.push_back(sink); },
                },
                predecessor);
        }
    }

    for (auto [id, descriptor, successors] : compiledQueryPlan.sources)
    {
        std::ranges::copy(instantiatedSinksWithSourcePredecessor[id], std::back_inserter(successors));
        instantiatedSources.emplace_back(
            NES::Sources::SourceProvider::lower(id, *descriptor, poolProvider, numberOfBuffersInSourceLocalBufferPool),
            std::move(successors));
    }


    return std::make_unique<ExecutableQueryPlan>(compiledQueryPlan.queryId, compiledQueryPlan.pipelines, std::move(instantiatedSources));
}

ExecutableQueryPlan::ExecutableQueryPlan(
    QueryId queryId,
    std::vector<std::shared_ptr<Execution::ExecutablePipeline>> pipelines,
    std::vector<SourceWithSuccessor> instantiatedSources)
    : queryId(queryId), pipelines(std::move(pipelines)), sources(std::move(instantiatedSources))
{
}
}
