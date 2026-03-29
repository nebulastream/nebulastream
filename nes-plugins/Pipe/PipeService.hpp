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

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <folly/MPMCQueue.h>
#include <folly/Synchronized.h>

class BackpressureController;

namespace NES
{

struct PipeEoS
{
};

struct PipeError
{
    std::string message;
};

using PipeChannelMessage = std::variant<PipeEoS, TupleBuffer, PipeError>;
using PipeQueue = folly::MPMCQueue<PipeChannelMessage>;

/// Central service for connecting pipe sinks and pipe sources by name.
/// A pipe sink registers under a name and fans out TupleBuffers to all registered pipe sources.
/// Sources register an MPMCQueue which receives data from the sink.
/// Sources can join and leave dynamically at runtime.
class PipeService
{
public:
    static PipeService& instance();

    /// Handle held by the PipeSink to fan-out buffers to all connected sources.
    /// Sources that register while data is flowing go into `pending` and are activated
    /// at the next sequence boundary to avoid partial data.
    /// Manages backpressure: blocks upstream source until at least one consumer is connected.
    struct SinkHandle
    {
        explicit SinkHandle(BackpressureController* bpController);

        struct ActiveConsumer
        {
            std::shared_ptr<PipeQueue> queue;
            SequenceNumber activatedAtSeq; /// sequence number at which this consumer was activated
        };

        struct Queues
        {
            std::vector<ActiveConsumer> active; /// receiving data
            std::vector<std::shared_ptr<PipeQueue>> pending; /// waiting for next sequence boundary
        };

        folly::Synchronized<Queues> queues;

        /// Add a consumer queue (goes to pending). Releases backpressure if this is the first consumer.
        void addConsumer(std::shared_ptr<PipeQueue> queue);

        /// Remove a consumer queue from active or pending. Applies backpressure if last consumer removed.
        void removeConsumer(const std::shared_ptr<PipeQueue>& queue);

    private:
        BackpressureController* bpController;
    };

    /// Register a sink under the given pipe name. Returns a SinkHandle for fan-out.
    /// The BackpressureController pointer is stored in the SinkHandle to manage backpressure
    /// based on consumer presence.
    /// Throws CannotOpenSink if a sink is already registered for this name.
    std::shared_ptr<SinkHandle>
    registerSink(const std::string& pipeName, const std::shared_ptr<const Schema>& schema, BackpressureController* bpController);

    /// Unregister a sink. Removes the pipe entry.
    void unregisterSink(const std::string& pipeName);

    /// Register a source under the given pipe name. Returns a queue that will receive data from the sink.
    /// The queue goes into pending and is activated at the next sequence boundary by the sink.
    /// Throws CannotOpenSource if no sink is registered for this name.
    /// Throws CannotOpenSource if the schema does not match the existing pipe schema.
    std::shared_ptr<PipeQueue> registerSource(const std::string& pipeName, const std::shared_ptr<const Schema>& schema);

    /// Unregister a source. Removes the queue from the SinkHandle (active or pending).
    void unregisterSource(const std::string& pipeName, const std::shared_ptr<PipeQueue>& queue);

private:
    PipeService() = default;

    static constexpr size_t DEFAULT_QUEUE_CAPACITY = 1024;

    struct PipeEntry
    {
        std::shared_ptr<const Schema> schema;
        std::shared_ptr<SinkHandle> sinkHandle; /// always non-null (sink must be registered first)
    };

    folly::Synchronized<std::unordered_map<std::string, PipeEntry>> pipes;
};

}
