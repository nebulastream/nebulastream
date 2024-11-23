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

#include <functional>
#include <memory>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Interfaces.hpp>
#include <RunningQueryPlan.hpp>
#include <RunningSource.hpp>

namespace NES::Runtime
{

namespace
{
Sources::SourceReturnType::EmitFunction emitFunction(
    QueryId queryId,
    std::weak_ptr<RunningSource> source,
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    QueryLifetimeController& controller,
    WorkEmitter& emitter)
{
    return [&controller, successors = std::move(successors), source, &emitter, queryId](
               OriginId sourceId, Sources::SourceReturnType::SourceReturnType event)
    {
        std::visit(
            Overloaded{
                [&](Sources::SourceReturnType::Data data)
                {
                    for (const auto& successor : successors)
                    {
                        emitter.emitWork(queryId, successor, std::move(data.buffer), {}, {});
                    }
                },
                [&](Sources::SourceReturnType::EoS) { controller.initializeSourceStop(queryId, sourceId, source); },
                [&](Sources::SourceReturnType::Error error)
                { controller.initializeSourceFailure(queryId, sourceId, source, std::move(error.ex)); }},
            std::move(event));
    };
}
}

OriginId RunningSource::getOriginId() const
{
    return source->getSourceId();
}
RunningSource::RunningSource(
    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
    std::unique_ptr<Sources::SourceHandle> source,
    std::function<void()> unregister,
    std::function<void(Exception)> unregisterWithError)
    : successors(std::move(successors))
    , source(std::move(source))
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
        if (!source->stop())
        {
            /// Currently we do not have to recover from a source which could not be properly closed as all our queries are one-shot.
            NES_WARNING("Could not gracefully stop source");
        }
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
