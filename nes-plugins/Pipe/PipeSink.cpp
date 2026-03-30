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
#include <PipeSink.hpp>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpptrace/from_current.hpp>
#include <ErrorHandling.hpp>
#include <PipeService.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

PipeSink::PipeSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , pipeName(sinkDescriptor.getFromConfig(ConfigParametersPipeSink::PIPE_NAME))
    , schema(sinkDescriptor.getSchema())
{
}

PipeSink::~PipeSink()
{
    CPPTRACE_TRY
    {
        if (sinkHandle)
        {
            NES_WARNING("PipeSink: destructor called with active sink handle for pipe '{}' — propagating error to consumers", pipeName);
            sinkHandle->queues.withRLock(
                [](const auto& queueState)
                {
                    for (const auto& consumer : queueState.active)
                    {
                        consumer.queue->blockingWrite(PipeChannelMessage{PipeError{"Pipe sink destroyed unexpectedly"}});
                    }
                    for (const auto& queue : queueState.pending)
                    {
                        queue->blockingWrite(PipeChannelMessage{PipeError{"Pipe sink destroyed unexpectedly"}});
                    }
                });
            PipeService::instance().unregisterSink(pipeName);
            sinkHandle.reset();
        }
    }
    CPPTRACE_CATCH(...)
    {
        NES_ERROR("PipeSink: exception in destructor for pipe '{}'", pipeName);
    }
}

void PipeSink::start(PipelineExecutionContext&)
{
    NES_INFO("PipeSink: starting for pipe '{}'", pipeName);
    /// Apply backpressure before registering — upstream source will be blocked until a consumer connects.
    backpressureController.applyPressure();
    sinkHandle = PipeService::instance().registerSink(pipeName, schema, &backpressureController);
}

void PipeSink::stop(PipelineExecutionContext&)
{
    NES_INFO("PipeSink: stopping for pipe '{}'", pipeName);
    if (sinkHandle)
    {
        /// Send EoS to all connected source queues (both active and pending)
        sinkHandle->queues.withRLock(
            [](const auto& queueState)
            {
                for (const auto& consumer : queueState.active)
                {
                    consumer.queue->blockingWrite(PipeChannelMessage{PipeEoS{}});
                }
                for (const auto& queue : queueState.pending)
                {
                    queue->blockingWrite(PipeChannelMessage{PipeEoS{}});
                }
            });
    }
    PipeService::instance().unregisterSink(pipeName);
    sinkHandle.reset();
    NES_INFO("PipeSink: stopped for pipe '{}'", pipeName);
}

void PipeSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in PipeSink.");
    if (!sinkHandle)
    {
        return;
    }

    const auto seqNum = inputTupleBuffer.getSequenceNumber();

    /// Only promote pending consumers when we see a truly new sequence —
    /// one we haven't delivered any chunks of yet. This prevents mid-sequence
    /// promotion which would cause the consumer to miss earlier chunks.
    bool needsPromotion = false;
    if (seqNum > maxSeqStarted)
    {
        sinkHandle->queues.withRLock([&](const auto& queueState) { needsPromotion = !queueState.pending.empty(); });
    }

    if (needsPromotion)
    {
        /// WLock: promote pending → active, then fan out in the same lock
        sinkHandle->queues.withWLock(
            [&](auto& queueState)
            {
                if (!queueState.pending.empty())
                {
                    NES_INFO(
                        "PipeSink: activating {} pending sources at sequence boundary (seq={}) on pipe '{}'",
                        queueState.pending.size(),
                        seqNum,
                        pipeName);
                    for (auto& pendingQueue : queueState.pending)
                    {
                        queueState.active.push_back({.queue = std::move(pendingQueue), .activatedAtSeq = seqNum});
                    }
                    queueState.pending.clear();
                }
                for (const auto& consumer : queueState.active)
                {
                    /// Only deliver buffers from sequences >= activation point.
                    /// OOO buffers from older sequences are dropped for this consumer.
                    if (seqNum >= consumer.activatedAtSeq)
                    {
                        consumer.queue->blockingWrite(PipeChannelMessage{inputTupleBuffer});
                    }
                }
            });
    }
    else
    {
        /// RLock: queues are thread-safe, we only need the lock to guard the vector
        sinkHandle->queues.withRLock(
            [&](const auto& queueState)
            {
                for (const auto& consumer : queueState.active)
                {
                    if (seqNum >= consumer.activatedAtSeq)
                    {
                        consumer.queue->blockingWrite(PipeChannelMessage{inputTupleBuffer});
                    }
                }
            });
    }

    if (seqNum > maxSeqStarted)
    {
        maxSeqStarted = seqNum;
    }
}

DescriptorConfig::Config PipeSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersPipeSink>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterPipeSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return PipeSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterPipeSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<PipeSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
