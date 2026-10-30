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

#include <algorithm>
#include <atomic>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <cpptrace/from_current.hpp> ///NOLINT(misc-include-cleaner)
#include <cpptrace/from_current_macros.hpp>
#include <ErrorHandling.hpp>
#include <PipeService.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
/// NOLINTNEXTLINE(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::string> PIPE_NAME{Identifier::parse("PIPE_NAME"), "The name of the pipe to write to."};
}

Schema<QualifiedErasedConfigField, Ordered> PipeSink::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("PIPE_SINK"), PIPE_NAME);
}

std::expected<PipeSinkConfig, Exception> PipeSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return PipeSinkConfig{.pipeName = config.get(PIPE_NAME)};
}

PipeSink::PipeSink(BackpressureController backpressureController, const PipeSinkConfig& config, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , pipeName(config.pipeName)
    , schema(NES::get<std::shared_ptr<const PipeSchema>>(sinkDescriptor.getSchema()))
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

Sink::BufferResult PipeSink::executeBuffer(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pec)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in PipeSink.");
    if (!sinkHandle)
    {
        return BufferResult::COMPLETED;
    }

    const std::lock_guard lock(deliveryMutex);
    if (pendingDelivery
        && (pendingDelivery->sequenceNumber != inputTupleBuffer.getSequenceNumber()
            || pendingDelivery->chunkNumber != inputTupleBuffer.getChunkNumber()))
    {
        buffered.push_back(inputTupleBuffer);
        return BufferResult::COMPLETED;
    }

    auto currentBuffer = std::optional(inputTupleBuffer);
    while (currentBuffer)
    {
        if (!tryDeliver(*currentBuffer))
        {
            sinkHandle->setConsumerQueueFull(true);
            pec.repeatTask(*currentBuffer, BACKPRESSURE_RETRY_INTERVAL);
            return BufferResult::RETRY;
        }

        if (buffered.empty())
        {
            if (sinkHandle->setConsumerQueueFull(false))
            {
                NES_WARNING("PipeSink: consumer queues available again on pipe '{}'; backpressure released", pipeName);
            }
            return BufferResult::COMPLETED;
        }
        currentBuffer = std::move(buffered.front());
        buffered.pop_front();
    }
    std::unreachable();
}

bool PipeSink::tryDeliver(const TupleBuffer& inputTupleBuffer)
{
    const auto seqNum = inputTupleBuffer.getSequenceNumber();
    const auto seqNumValue = seqNum.getRawValue();

    const bool firstAttempt = !pendingDelivery;
    if (firstAttempt)
    {
        pendingDelivery.emplace(seqNum, inputTupleBuffer.getChunkNumber(), std::vector<std::shared_ptr<PipeQueue>>{});
    }

    /// Only promote pending consumers when we see a truly new sequence —
    /// one we haven't delivered any chunks of yet. This prevents mid-sequence
    /// promotion which would cause the consumer to miss earlier chunks.
    bool needsPromotion = false;
    if (seqNumValue > maxSeqStarted.load(std::memory_order_relaxed))
    {
        sinkHandle->queues.withRLock([&](const auto& queueState) { needsPromotion = !queueState.pending.empty(); });
    }

    bool delivered = true;
    sinkHandle->queues.withWLock(
        [&](auto& queueState)
        {
            if (needsPromotion && !queueState.pending.empty())
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
                if (seqNum < consumer.activatedAtSeq
                    || std::ranges::find(pendingDelivery->deliveredConsumers, consumer.queue) != pendingDelivery->deliveredConsumers.end())
                {
                    continue;
                }
                if (!consumer.queue->writeIfNotFull(PipeChannelMessage{inputTupleBuffer}))
                {
                    if (firstAttempt)
                    {
                        NES_WARNING(
                            "PipeSink: consumer queue full on pipe '{}' (capacity={}); applying backpressure and retrying buffer {}-{}",
                            pipeName,
                            consumer.queue->capacity(),
                            seqNum,
                            inputTupleBuffer.getChunkNumber());
                    }
                    delivered = false;
                    return;
                }
                pendingDelivery->deliveredConsumers.push_back(consumer.queue);
            }
        });

    if (!delivered)
    {
        return false;
    }

    pendingDelivery.reset();
    updateMaxSequence(seqNum);
    return true;
}

void PipeSink::updateMaxSequence(SequenceNumber sequenceNumber)
{
    const auto seqNumValue = sequenceNumber.getRawValue();
    auto maxSeq = maxSeqStarted.load(std::memory_order_relaxed);
    while (seqNumValue > maxSeq)
    {
        if (maxSeqStarted.compare_exchange_weak(maxSeq, seqNumValue, std::memory_order_relaxed, std::memory_order_relaxed))
        {
            break;
        }
    }
}

}
