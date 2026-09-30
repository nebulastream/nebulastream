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

#include <AsyncSource.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <map>
#include <ranges>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Async/AsyncWiring.hpp>
#include <Async/HandoffChannel.hpp>
#include <AsyncExecutorRegistry.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <fmt/format.h>

namespace NES
{

AsyncSource::AsyncSource(const SourceDescriptor& sourceDescriptor)
    : channelId(sourceDescriptor.getFromConfig(ConfigParametersAsyncSource::CHANNEL))
    , channelCapacity(sourceDescriptor.getFromConfig(ConfigParametersAsyncSource::CHANNEL_CAPACITY))
    , executorType(sourceDescriptor.getFromConfig(ConfigParametersAsyncSource::EXECUTOR_TYPE))
    , executorConfig(decodeConfig(sourceDescriptor.getFromConfig(ConfigParametersAsyncSource::EXECUTOR_CONFIG)))
    , batchSize(std::max<size_t>(1, sourceDescriptor.getFromConfig(ConfigParametersAsyncSource::BATCH_SIZE)))
    , maxConcurrency(std::max<size_t>(1, sourceDescriptor.getFromConfig(ConfigParametersAsyncSource::MAX_CONCURRENCY)))
    , preserveOrder(sourceDescriptor.getFromConfig(ConfigParametersAsyncSource::PRESERVE_ORDER))
    , inputLayout(decodeSchema(sourceDescriptor.getFromConfig(ConfigParametersAsyncSource::INPUT_SCHEMA)))
    , outputLayout(*sourceDescriptor.getLogicalSource().getSchema())
{
}

AsyncSource::~AsyncSource()
{
    /// close() is the orderly path; this only catches the case where it never ran.
    workerStop.request_stop();
}

void AsyncSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    /// Either half may start first, so whoever gets here first creates the channel.
    channel = HandoffChannelRegistry::getOrCreate(channelId, channelCapacity);

    const auto factory = AsyncExecutorRegistry::instance().find(executorType);
    if (!factory.has_value())
    {
        throw InvalidConfigParameter("No asynchronous executor is registered under '{}'", executorType);
    }
    executor = (*factory)(
        AsyncExecutorRegistryArguments{
            AsyncOperatorContext{
                .inputLayout = &inputLayout, .outputLayout = &outputLayout, .config = executorConfig, .batchSize = batchSize}});

    {
        const std::lock_guard lock(mutex);
        activeWorkers = maxConcurrency;
    }

    workers.reserve(maxConcurrency);
    for (size_t worker = 0; worker < maxConcurrency; ++worker)
    {
        workers.emplace_back(
            fmt::format("AsyncOp-{}-{}", executorType, worker), [this](const std::stop_token& stopToken) { processingLoop(stopToken); });
    }
    NES_DEBUG("Async source for '{}' started {} worker(s) on channel {}", executorType, maxConcurrency, channelId);
}

void AsyncSource::processingLoop(const std::stop_token& stopToken)
{
    /// The engine's own stop token reaches us through close(); this one covers both.
    const std::stop_callback forwardStop(stopToken, [this] { workerStop.request_stop(); });

    while (true)
    {
        std::optional<TupleBuffer> input;
        uint64_t ticket = 0;
        {
            /// Taking a buffer and taking a ticket must happen together, otherwise two
            /// workers could number their buffers in the opposite order to the one they
            /// received them in, and `preserveOrder` would order the wrong thing.
            const std::lock_guard popLock(popMutex);
            input = channel->popBlocking(workerStop.get_token());
            if (!input.has_value())
            {
                break;
            }
            ticket = nextTicket++;
        }

        /// This is where the slow external call happens — on this thread, never on a worker
        /// thread of the engine.
        const auto recordCount = input->getNumberOfTuples();
        std::vector<AsyncRecordResult> results;
        results.reserve(recordCount);

        for (uint64_t offset = 0; offset < recordCount; offset += batchSize)
        {
            const auto batchEnd = std::min<uint64_t>(offset + batchSize, recordCount);
            std::vector<AsyncRecordView> batch;
            batch.reserve(batchEnd - offset);
            for (uint64_t index = offset; index < batchEnd; ++index)
            {
                batch.emplace_back(inputLayout, *input, index);
            }

            auto batchResults = executor->process(batch);
            if (batchResults.size() != batch.size())
            {
                throw UnsupportedQuery(
                    "Executor '{}' returned {} results for {} records; results must align with the input",
                    executorType,
                    batchResults.size(),
                    batch.size());
            }
            std::ranges::move(batchResults, std::back_inserter(results));
        }

        {
            const std::lock_guard lock(mutex);
            completed.emplace(ticket, Processed{.input = std::move(*input), .results = std::move(results), .emittedRecords = 0});
        }
        completedArrived.notify_all();
    }

    {
        const std::lock_guard lock(mutex);
        --activeWorkers;
        inputExhausted = activeWorkers == 0;
    }
    completedArrived.notify_all();
}

std::optional<AsyncSource::Processed> AsyncSource::takeNextCompleted(const std::stop_token& stopToken)
{
    std::unique_lock lock(mutex);

    if (preserveOrder)
    {
        completedArrived.wait(lock, stopToken, [this] { return completed.contains(nextToEmit) || inputExhausted; });
        if (const auto it = completed.find(nextToEmit); it != completed.end())
        {
            auto processed = std::move(it->second);
            completed.erase(it);
            ++nextToEmit;
            return processed;
        }
        return std::nullopt;
    }

    completedArrived.wait(lock, stopToken, [this] { return not completed.empty() || inputExhausted; });
    if (not completed.empty())
    {
        auto it = completed.begin();
        auto processed = std::move(it->second);
        completed.erase(it);
        return processed;
    }
    return std::nullopt;
}

void AsyncSource::assignChunkNumber(const bool isEndOfIncomingChunk, const TupleBuffer& input, TupleBuffer& output)
{
    /// Mirrors EmitOperatorHandler::setChunkNumber. One incoming buffer can become several
    /// outgoing ones — the records grew — so chunk numbers have to be handed out per
    /// (sequence, origin) and exactly one buffer may carry the last-chunk flag.
    const SequenceKey key{input.getSequenceNumber().getRawValue(), input.getOriginId().getRawValue()};
    auto& state = sequenceStates[key];

    output.setChunkNumber(ChunkNumber(state.nextChunkNumber));
    state.nextChunkNumber += 1;
    output.setLastChunk(false);

    if (!isEndOfIncomingChunk)
    {
        return;
    }

    if (input.isLastChunk())
    {
        INVARIANT(
            state.lastIncomingChunk == INVALID<ChunkNumber>,
            "Received a second last chunk for sequence {} of origin {}",
            key.first,
            key.second);
        state.lastIncomingChunk = input.getChunkNumber();
    }
    state.seenIncomingChunks += 1;

    if (state.lastIncomingChunk != INVALID<ChunkNumber>
        && state.seenIncomingChunks - 1 == state.lastIncomingChunk.getRawValue() - ChunkNumber::INITIAL)
    {
        sequenceStates.erase(key);
        output.setLastChunk(true);
    }
}

Source::FillTupleBufferResult AsyncSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    if (!partiallyEmitted.has_value())
    {
        partiallyEmitted = takeNextCompleted(stopToken);
    }
    if (!partiallyEmitted.has_value())
    {
        /// Nothing left. Whether this is the end of the stream or a requested stop is decided
        /// by SourceThread, which suppresses end-of-stream when a stop was asked for.
        return FillTupleBufferResult::eos();
    }

    auto& processed = *partiallyEmitted;
    const auto totalRecords = processed.results.size();
    const auto capacity = outputLayout.capacity(tupleBuffer.getBufferSize());
    INVARIANT(capacity > 0, "Output records do not fit into a single buffer");

    const auto remaining = totalRecords - processed.emittedRecords;
    const auto toEmit = std::min<uint64_t>(capacity, remaining);

    for (uint64_t index = 0; index < toEmit; ++index)
    {
        const auto sourceIndex = processed.emittedRecords + index;
        const AsyncRecordView inputRecord{inputLayout, processed.input, sourceIndex};
        AsyncRecordWriter writer{outputLayout, tupleBuffer, *bufferProvider, index};

        /// Carry the incoming fields over, then apply whatever the operator produced. A field
        /// the executor left out keeps its default, so a record is never dropped.
        writer.copyMatchingFields(inputRecord);
        for (const auto& [fieldIndex, value] : processed.results[sourceIndex].fields)
        {
            writer.writeAsText(fieldIndex, value);
        }
    }

    processed.emittedRecords += toEmit;
    const bool finishedInputBuffer = processed.emittedRecords == totalRecords;

    /// Metadata is carried over verbatim; renumbering here would break the downstream
    /// watermark processing. Only the chunk number is ours to assign.
    tupleBuffer.setNumberOfTuples(toEmit);
    tupleBuffer.setOriginId(processed.input.getOriginId());
    tupleBuffer.setSequenceNumber(processed.input.getSequenceNumber());
    tupleBuffer.setWatermark(processed.input.getWatermark());
    assignChunkNumber(finishedInputBuffer, processed.input, tupleBuffer);

    if (finishedInputBuffer)
    {
        partiallyEmitted.reset();
    }

    return FillTupleBufferResult::withBytes(toEmit);
}

void AsyncSource::close()
{
    workerStop.request_stop();
    /// Thread's destructor joins, so clearing waits for every worker to leave its loop.
    workers.clear();
    executor.reset();
    channel.reset();
    NES_DEBUG("Async source for '{}' stopped", executorType);
}

DescriptorConfig::Config AsyncSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersAsyncSource>(std::move(config), name());
}

std::ostream& AsyncSource::toString(std::ostream& str) const
{
    return str << "AsyncSource(executor: " << executorType << ", channel: " << channelId << ")";
}

}
