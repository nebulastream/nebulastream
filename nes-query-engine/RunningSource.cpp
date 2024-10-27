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
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Interfaces.hpp>
#include <QueryEngine.hpp>
#include <RunningQueryPlan.hpp>
#include <RunningSource.hpp>
#include <Task.hpp>

namespace NES::Runtime
{

template <typename... Ts>
struct overloaded : Ts...
{
    using Ts::operator()...;
};

static Sources::SourceReturnType::EmitFunction emitFunction(
    QueryId queryId,
    std::weak_ptr<RunningSource> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    using namespace Sources::SourceReturnType;
    return [&controller, successors = std::move(successors), source, &emitter, queryId](OriginId sourceId, SourceReturnType event)
    {
        std::visit(
            overloaded{
                [&](Data&& data)
                {
                    for (const auto& successor : successors)
                    {
                        emitter.emitWork(queryId, successor, data.buffer);
                    }
                },
                [&](EoS) { controller.initializeSourceStop(queryId, sourceId, source); },
                [&](Error&& e) { controller.initializeSourceFailure(queryId, sourceId, source, std::move(e.ex)); }},
            std::move(event));
    };
}

OriginId RunningSource::getOriginId() const
{
    return source->getSourceId();
}
RunningSource::RunningSource(
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::unique_ptr<Sources::SourceHandle> pSource,
    std::function<void()> unregister,
    std::function<void(Exception)> unregisterWithError)
    : successors(std::move(successors))
    , source(std::move(pSource))
    , unregister(std::move(unregister))
    , unregisterWithError(std::move(unregisterWithError))
{
}

std::shared_ptr<RunningSource> RunningSource::create(
    QueryId queryId,
    std::unique_ptr<Sources::SourceHandle> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::function<void()> unregister,
    std::function<void(Exception)> unregisterWithError,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    auto runningSource = std::shared_ptr<RunningSource>(
        new RunningSource(successors, std::move(source), std::move(unregister), std::move(unregisterWithError)));
    NES_DEBUG("Starting Running Source");
    runningSource->source->start(emitFunction(queryId, runningSource, std::move(successors), controller, emitter));
    return runningSource;
}

RunningSource::~RunningSource()
{
    if (source)
    {
        NES_DEBUG("Stopping Running Source");
        source->stop();
    }
}
void RunningSource::stop() const
{
    unregister();
}
void RunningSource::fail(Exception exception) const
{
    unregisterWithError(std::move(exception));
}

}
