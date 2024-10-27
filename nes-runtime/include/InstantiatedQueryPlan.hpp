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
#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <vector>
#include <Sinks/SinkProvider.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <ExecutableQueryPlan.hpp>

template <typename... U>
struct Overloaded : U...
{
    using U::operator()...;
};

namespace NES::Runtime
{
struct InstantiatedQueryPlan
{
    static std::unique_ptr<InstantiatedQueryPlan> instantiate(
        std::unique_ptr<Execution::ExecutableQueryPlan> executableQueryPlan, std::shared_ptr<Memory::AbstractPoolProvider> poolProvider)
    {
        std::vector<std::pair<std::unique_ptr<Sources::SourceHandle>, std::vector<std::weak_ptr<Execution::ExecutablePipeline>>>>
            instantiatedSources;

        std::unordered_map<OriginId, std::vector<Execution::ExecutablePipelinePtr>> instantiatedSinksWithSourcePredecessor;

        for (auto& [descriptor, predecessor] : executableQueryPlan->sinks)
        {
            auto sink = Execution::ExecutablePipeline::create(Sinks::SinkProvider::lower(*descriptor), {});
            std::visit(
                Overloaded{
                    [&](OriginId source) { instantiatedSinksWithSourcePredecessor[source].push_back(sink); },
                    [&](std::weak_ptr<Execution::ExecutablePipeline> pipeline) { pipeline.lock()->successors.push_back(sink); },
                },
                predecessor);
        }

        for (auto [descriptor, id, successors] : executableQueryPlan->sources)
        {
            std::ranges::copy(instantiatedSinksWithSourcePredecessor[id], std::back_inserter(successors));
            instantiatedSources.emplace_back(NES::Sources::SourceProvider::lower(id, *descriptor, poolProvider), std::move(successors));
        }


        return std::make_unique<InstantiatedQueryPlan>(std::move(executableQueryPlan->pipelines), std::move(instantiatedSources));
    }

    InstantiatedQueryPlan(
        std::vector<std::shared_ptr<Execution::ExecutablePipeline>> pipelines,
        std::vector<std::pair<std::unique_ptr<Sources::SourceHandle>, std::vector<std::weak_ptr<Execution::ExecutablePipeline>>>>
            instantiated_sources)
        : pipelines(std::move(pipelines)), sources(std::move(instantiated_sources))
    {
    }

    std::vector<std::shared_ptr<Execution::ExecutablePipeline>> pipelines;
    std::vector<std::pair<std::unique_ptr<Sources::SourceHandle>, std::vector<std::weak_ptr<Execution::ExecutablePipeline>>>> sources;
};
}
