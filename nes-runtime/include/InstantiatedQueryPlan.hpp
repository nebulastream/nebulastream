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
#include <memory>
#include <ostream>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ExecutableQueryPlan.hpp>

namespace NES::Runtime
{

/// The InstantiatedQueryPlan represents a query with completely instantiated query processing components (Sources, Pipelines, Sinks).
/// In this form the Query could be executed, by starting all pipelines, sinks and passing the successor pipelines into the queries sources.
struct InstantiatedQueryPlan
{
    using SourceWithSuccessor
        = std::pair<std::unique_ptr<Sources::SourceHandle>, std::vector<std::weak_ptr<Execution::ExecutablePipeline>>>;
    static std::unique_ptr<InstantiatedQueryPlan> instantiate(
        std::unique_ptr<Execution::ExecutableQueryPlan>&& executableQueryPlan,
        const std::shared_ptr<Memory::AbstractPoolProvider>& poolProvider);

    InstantiatedQueryPlan(
        QueryId queryId,
        std::vector<std::shared_ptr<Execution::ExecutablePipeline>> pipelines,
        std::vector<SourceWithSuccessor> instantiatedSources);

    QueryId queryId;
    std::vector<std::shared_ptr<Execution::ExecutablePipeline>> pipelines;
    std::vector<SourceWithSuccessor> sources;
    friend std::ostream& operator<<(std::ostream& os, const InstantiatedQueryPlan& instantiatedQueryPlan);
};
}

FMT_OSTREAM(NES::Runtime::InstantiatedQueryPlan);
