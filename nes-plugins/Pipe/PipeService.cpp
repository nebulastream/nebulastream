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

#include <PipeService.hpp>

#include <memory>
#include <string>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>

namespace
{
/// Normalize a schema by stripping source qualifiers so that e.g. "GEN_SOURCE$id" and "id" compare equal.
std::shared_ptr<const NES::Schema> normalizeSchema(const std::shared_ptr<const NES::Schema>& schema)
{
    return std::make_shared<const NES::Schema>(NES::withoutSourceQualifier(*schema));
}
}

namespace NES
{

/// --- SinkHandle ---

PipeService::SinkHandle::SinkHandle(BackpressureController* bpController) : bpController(bpController)
{
}

void PipeService::SinkHandle::addConsumer(std::shared_ptr<PipeQueue> queue)
{
    queues.withWLock(
        [&](auto& queues)
        {
            const bool wasEmpty = queues.active.empty() && queues.pending.empty();
            queues.pending.push_back(std::move(queue));
            if (wasEmpty && bpController)
            {
                bpController->releasePressure();
            }
        });
}

void PipeService::SinkHandle::removeConsumer(const std::shared_ptr<PipeQueue>& queue)
{
    queues.withWLock(
        [&](auto& queues)
        {
            std::erase_if(queues.active, [&](const auto& consumer) { return consumer.queue == queue; });
            std::erase(queues.pending, queue);
            if (queues.active.empty() && queues.pending.empty() && bpController)
            {
                bpController->applyPressure();
            }
        });
}

/// --- PipeService ---

PipeService& PipeService::instance()
{
    static PipeService service;
    return service;
}

std::shared_ptr<PipeService::SinkHandle>
PipeService::registerSink(const std::string& pipeName, const std::shared_ptr<const Schema>& schema, BackpressureController* bpController)
{
    auto normalizedSchema = normalizeSchema(schema);
    return pipes.withWLock(
        [&](auto& map) -> std::shared_ptr<SinkHandle>
        {
            auto it = map.find(pipeName);
            if (it != map.end())
            {
                throw CannotOpenSink("Pipe sink already registered for name '{}'", pipeName);
            }
            auto sinkHandle = std::make_shared<SinkHandle>(bpController);
            map.emplace(pipeName, PipeEntry{.schema = std::move(normalizedSchema), .sinkHandle = sinkHandle});
            NES_INFO("PipeService: registered sink for pipe '{}'", pipeName);
            return sinkHandle;
        });
}

void PipeService::unregisterSink(const std::string& pipeName)
{
    pipes.withWLock(
        [&](auto& map)
        {
            auto it = map.find(pipeName);
            if (it != map.end())
            {
                NES_INFO("PipeService: unregistered sink for pipe '{}'", pipeName);
                map.erase(it);
            }
        });
}

std::shared_ptr<PipeQueue> PipeService::registerSource(const std::string& pipeName, const std::shared_ptr<const Schema>& schema)
{
    auto normalizedSchema = normalizeSchema(schema);
    return pipes.withWLock(
        [&](auto& map) -> std::shared_ptr<PipeQueue>
        {
            auto queue = std::make_shared<PipeQueue>(DEFAULT_QUEUE_CAPACITY);
            auto it = map.find(pipeName);
            if (it != map.end())
            {
                auto& entry = it->second;
                /// Verify schema match
                if (*entry.schema != *normalizedSchema)
                {
                    throw CannotOpenSource("Schema mismatch for pipe '{}': source schema does not match existing pipe schema", pipeName);
                }
                INVARIANT(entry.sinkHandle, "PipeEntry exists but sinkHandle is null for pipe '{}'", pipeName);
                entry.sinkHandle->addConsumer(queue);
                NES_INFO("PipeService: registered source for pipe '{}' (pending activation at next sequence boundary)", pipeName);
                return queue;
            }
            /// No entry for this pipe name — no sink has ever been registered
            throw CannotOpenSource("No pipe sink registered for name '{}'", pipeName);
        });
}

void PipeService::unregisterSource(const std::string& pipeName, const std::shared_ptr<PipeQueue>& queue)
{
    pipes.withWLock(
        [&](auto& map)
        {
            auto it = map.find(pipeName);
            if (it == map.end())
            {
                return;
            }
            auto& entry = it->second;
            entry.sinkHandle->removeConsumer(queue);
            NES_INFO("PipeService: unregistered source for pipe '{}'", pipeName);
        });
}

}
