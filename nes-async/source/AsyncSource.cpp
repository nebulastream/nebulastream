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
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <exception>
#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Async/AsyncWiring.hpp>
#include <Async/HandoffChannel.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <AsyncExecutorRegistry.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

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
    /// Taken here, at construction, and not in open(). Construction happens synchronously while
    /// the consumer's plan is started, and AsyncOperatorSplitter starts that plan before its
    /// producer's. So the channel exists, held by its reader, before anything can be written
    /// into it. Taking it in open() -- on the source thread, some time later -- let a producer
    /// with little input push everything, close the channel and be torn down first; the channel
    /// went with it, since the registry only holds it weakly, and the consumer then created a
    /// fresh one and waited on it forever. The interpreter's timing made that likely enough to
    /// hang most asynchronous queries in interpreter mode.
    , channel(HandoffChannelRegistry::getOrCreate(channelId, channelCapacity))
{
}

AsyncSource::~AsyncSource()
{
    /// close() is the orderly path; this only catches the case where it never ran -- a deployment
    /// cancelled after this source was constructed but before it was opened. The producer must
    /// still learn that nobody will read, or it retries a full channel forever.
    if (channel != nullptr)
    {
        channel->closeConsumer();
    }
    workerStop.request_stop();
}

void AsyncSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    INVARIANT(channel != nullptr, "The handoff channel {} is taken at construction and released only by close()", channelId);

    const auto factory = AsyncExecutorRegistry::instance().find(executorType);
    if (!factory.has_value())
    {
        throw InvalidConfigParameter("No asynchronous executor is registered under '{}'", executorType);
    }
    executor = (*factory)(AsyncExecutorRegistryArguments{AsyncOperatorContext{
        .inputLayout = &inputLayout, .outputLayout = &outputLayout, .config = executorConfig, .batchSize = batchSize}});

    blockingExecutor = dynamic_cast<BlockingAsyncExecutor*>(executor.get());
    submittingExecutor = dynamic_cast<EventLoopAsyncExecutor*>(executor.get());
    if (blockingExecutor == nullptr && submittingExecutor == nullptr)
    {
        throw InvalidConfigParameter(
            "Asynchronous executor '{}' implements neither BlockingAsyncExecutor nor EventLoopAsyncExecutor", executorType);
    }

    /// Read-ahead is bounded by queued work, not by buffers: two batches per concurrent call keeps
    /// every worker fed without pulling the channel into memory. One large buffer usually yields
    /// enough batches on its own, so this normally parks one or two buffers. The buffer count is
    /// only a backstop for the opposite case, many tiny buffers, because each parked buffer is one
    /// the shared pool cannot hand out.
    queuedWorkHighWatermark = 2 * maxConcurrency;
    maxPendingBuffers = 2 * maxConcurrency;

    /// Only the blocking mode needs threads, one per call it may have in flight. An event-loop
    /// executor gets none: `pump` hands it up to `maxConcurrency` batches and the completions ask
    /// for the next one, so the calls in flight are bounded by that number and not by a thread
    /// count. At a concurrency of 256, that is the difference between 257 threads and two.
    if (blockingExecutor != nullptr)
    {
        workers.reserve(maxConcurrency);
        for (size_t worker = 0; worker < maxConcurrency; ++worker)
        {
            workers.emplace_back(
                fmt::format("AsyncOp-{}-{}", executorType, worker),
                [this](const std::stop_token& stopToken) { processingLoop(stopToken); });
        }
    }
    intake.emplace(fmt::format("AsyncIn-{}", executorType), [this](const std::stop_token& stopToken) { intakeLoop(stopToken); });

    NES_DEBUG(
        "Async source for '{}' opened on channel {} with up to {} calls in flight, {}",
        executorType,
        channelId,
        maxConcurrency,
        blockingExecutor != nullptr ? "one thread each" : "all on one event loop");
}

void AsyncSource::recordFailure(std::exception_ptr reason)
{
    {
        const std::lock_guard lock(mutex);
        /// The first reason is the interesting one; later ones are usually consequences of it.
        if (!failure)
        {
            failure = std::move(reason);
        }
        /// Nothing is going to finish the outstanding work now, so let every waiter out: the
        /// workers leave their loops and fillTupleBuffer stops waiting for a buffer that will
        /// never complete.
        intakeDone = true;
        inputExhausted = true;
    }
    /// The query is failing, so nothing will read the channel again; let the producer know now
    /// rather than once the engine gets round to close().
    channel->closeConsumer();
    workerStop.request_stop();
    workArrived.notify_all();
    completedArrived.notify_all();
}

void AsyncSource::rethrowIfFailed()
{
    std::exception_ptr reason;
    {
        const std::lock_guard lock(mutex);
        reason = failure;
    }
    if (reason)
    {
        std::rethrow_exception(reason);
    }
}

void AsyncSource::intakeLoop(const std::stop_token& stopToken)
{
    /// The engine's own stop token reaches us through close(); this one covers both.
    const std::stop_callback forwardStop(stopToken, [this] { workerStop.request_stop(); });
    const auto token = workerStop.get_token();

    try
    {
        while (true)
        {
            {
                /// Never pull more of the channel in than the workers can chew on: every pending
                /// buffer is one the shared pool cannot hand out, and a timeout there fails the query.
                std::unique_lock lock(mutex);
                completedArrived.wait(
                    lock, token, [this] { return work.size() < queuedWorkHighWatermark && pending.size() < maxPendingBuffers; });
                if (token.stop_requested())
                {
                    break;
                }
            }

            auto input = channel->popBlocking(token);
            if (!input.has_value())
            {
                break;
            }

            const auto recordCount = input->getNumberOfTuples();
            {
                const std::lock_guard lock(mutex);
                const auto ticket = nextTicket++;

                if (recordCount == 0)
                {
                    /// No batches means nothing would ever complete this buffer, so it is done now.
                    completed.emplace(ticket, Processed{.input = std::move(*input), .results = {}, .emittedRecords = 0});
                }
                else
                {
                    auto& entry = pending[ticket];
                    entry.input = std::move(*input);
                    /// Sized once and never again, so every batch can write its own range of it
                    /// without holding the lock.
                    entry.results.resize(recordCount);
                    for (uint64_t offset = 0; offset < recordCount; offset += batchSize)
                    {
                        work.push_back(
                            BatchWork{.ticket = ticket, .begin = offset, .end = std::min<uint64_t>(offset + batchSize, recordCount)});
                        entry.outstandingBatches += 1;
                    }
                }
            }
            workArrived.notify_all();
            completedArrived.notify_all();
            if (submittingExecutor != nullptr)
            {
                pump();
            }
        }

        {
            const std::lock_guard lock(mutex);
            intakeDone = true;
            /// Only ever set, never cleared. The flag means "nothing will complete any more", and a
            /// failure recorded while this thread was reading has already set it; assigning here
            /// would take it back and leave fillTupleBuffer waiting for a result nobody will produce.
            inputExhausted = inputExhausted || (work.empty() && pending.empty());
        }
        /// Both, because idle workers wait on one and fillTupleBuffer on the other.
        workArrived.notify_all();
        completedArrived.notify_all();
        if (submittingExecutor != nullptr)
        {
            /// Whatever is still queued when the input ends has to go out, or nothing completes it.
            pump();
        }
    }
    catch (...)
    {
        recordFailure(std::current_exception());
    }
}

void AsyncSource::pump()
{
    std::vector<std::shared_ptr<SubmittedBatch>> toSubmit;
    {
        const std::lock_guard lock(mutex);
        while (callsInFlight < maxConcurrency && not work.empty() && not failure && not workerStop.stop_requested())
        {
            const auto item = work.front();
            work.pop_front();

            auto& entry = pending.at(item.ticket);
            auto submitted = std::make_shared<SubmittedBatch>();
            submitted->ticket = item.ticket;
            /// The buffer handle keeps the records alive for the duration of the call, and the
            /// result slots are this batch's own disjoint range of the ticket's results.
            submitted->input = entry.input;
            submitted->resultSlots = entry.results.data() + item.begin;
            submitted->views.reserve(item.end - item.begin);
            for (uint64_t index = item.begin; index < item.end; ++index)
            {
                submitted->views.emplace_back(inputLayout, submitted->input, index);
            }

            callsInFlight += 1;
            toSubmit.push_back(std::move(submitted));
        }
    }

    /// Submitted without the lock: an executor is allowed to answer inline — when its loop has
    /// already been stopped, say — and the completion takes this very mutex.
    for (auto& submitted : toSubmit)
    {
        const auto batch = std::span<const AsyncRecordView>{submitted->views};
        submittingExecutor->submit(batch, [this, submitted](AsyncBatchOutcome outcome) { onBatchFinished(submitted, std::move(outcome)); });
    }
}

void AsyncSource::onBatchFinished(const std::shared_ptr<SubmittedBatch>& submitted, AsyncBatchOutcome outcome)
{
    /// Runs on a thread of the executor's event loop, never on one of ours, so nothing may escape
    /// from here: the loop has no handler either.
    if (not outcome.has_value())
    {
        {
            const std::lock_guard lock(mutex);
            callsInFlight -= 1;
        }
        recordFailure(outcome.error());
        return;
    }

    if (outcome->size() != submitted->views.size())
    {
        {
            const std::lock_guard lock(mutex);
            callsInFlight -= 1;
        }
        recordFailure(std::make_exception_ptr(UnsupportedQuery(
            "Executor '{}' returned {} results for {} records; results must align with the input",
            executorType,
            outcome->size(),
            submitted->views.size())));
        return;
    }

    {
        const std::lock_guard lock(mutex);
        callsInFlight -= 1;
        std::ranges::move(outcome.value(), submitted->resultSlots);

        const auto entryIterator = pending.find(submitted->ticket);
        INVARIANT(entryIterator != pending.end(), "A batch completed for a buffer that is no longer pending");
        entryIterator->second.outstandingBatches -= 1;
        if (entryIterator->second.outstandingBatches == 0)
        {
            completed.emplace(
                submitted->ticket,
                Processed{
                    .input = std::move(entryIterator->second.input),
                    .results = std::move(entryIterator->second.results),
                    .emittedRecords = 0});
            pending.erase(entryIterator);
            inputExhausted = inputExhausted || (intakeDone && work.empty() && pending.empty());
        }
    }
    /// Wakes fillTupleBuffer, and the intake thread, for which a slot may just have freed up.
    completedArrived.notify_all();
    pump();
}

std::optional<AsyncSource::BatchWork> AsyncSource::takeNextBatch(std::unique_lock<std::mutex>& lock, const std::stop_token& stopToken)
{
    workArrived.wait(lock, stopToken, [this] { return not work.empty() || intakeDone || failure; });
    /// The wait returns as soon as its predicate holds, stop or not, so a stop with batches still
    /// queued has to be checked separately. Taking them would mean one more model call each — up
    /// to the request timeout apiece — whose results nobody will emit, while close() waits.
    /// A stop or a failure abandons whatever is still queued or in flight; an empty queue merely
    /// means this worker has nothing left to do, while others may still be finishing theirs.
    const bool abandoning = stopToken.stop_requested() || failure;
    if (work.empty() || abandoning)
    {
        if (abandoning && (not work.empty() || not pending.empty()))
        {
            /// Nobody is going to finish those buffers now, so say so. Without this the source
            /// thread waits for a buffer whose last worker has already left.
            inputExhausted = true;
            completedArrived.notify_all();
        }
        /// Otherwise the last worker to drain `pending` sets the flag, which is the ordinary end
        /// of the stream — declaring it here would cut off the buffers still being worked on.
        return std::nullopt;
    }
    const auto item = work.front();
    work.pop_front();
    return item;
}

void AsyncSource::processingLoop(const std::stop_token& stopToken)
{
    const std::stop_callback forwardStop(stopToken, [this] { workerStop.request_stop(); });
    const auto token = workerStop.get_token();

    try
    {
        while (true)
        {
            std::unique_lock lock(mutex);
            const auto item = takeNextBatch(lock, token);
            if (!item.has_value())
            {
                break;
            }

            /// `pending` is node-based, so this reference stays valid while other tickets come and go,
            /// and the buffer handle is copied to keep the records alive for the duration of the call.
            auto& entry = pending.at(item->ticket);
            const TupleBuffer input = entry.input;
            auto* const resultSlots = entry.results.data() + item->begin;
            lock.unlock();

            std::vector<AsyncRecordView> batch;
            batch.reserve(item->end - item->begin);
            for (uint64_t index = item->begin; index < item->end; ++index)
            {
                batch.emplace_back(inputLayout, input, index);
            }

            /// This is where the slow external call happens — on this thread, never on a worker
            /// thread of the engine. One batch is one call, so `maxConcurrency` bounds calls in
            /// flight rather than input buffers.
            auto batchResults = blockingExecutor->process(batch);
            if (batchResults.size() != batch.size())
            {
                throw UnsupportedQuery(
                    "Executor '{}' returned {} results for {} records; results must align with the input",
                    executorType,
                    batchResults.size(),
                    batch.size());
            }
            std::ranges::move(batchResults, resultSlots);

            lock.lock();
            const auto entryIterator = pending.find(item->ticket);
            INVARIANT(entryIterator != pending.end(), "A batch completed for a buffer that is no longer pending");
            entryIterator->second.outstandingBatches -= 1;
            if (entryIterator->second.outstandingBatches == 0)
            {
                completed.emplace(
                    item->ticket,
                    Processed{
                        .input = std::move(entryIterator->second.input),
                        .results = std::move(entryIterator->second.results),
                        .emittedRecords = 0});
                pending.erase(entryIterator);
                inputExhausted = inputExhausted || (intakeDone && work.empty() && pending.empty());
                lock.unlock();
                /// Wakes fillTupleBuffer, and the intake thread, for which a slot just freed up.
                completedArrived.notify_all();
                continue;
            }
            lock.unlock();
        }
    }
    catch (...)
    {
        /// Most often a transport failure the executor gave up on. The synchronous operator fails
        /// the query for the same cause, which is what makes the two modes comparable.
        recordFailure(std::current_exception());
    }
}

std::optional<AsyncSource::Processed> AsyncSource::takeNextCompleted(const std::stop_token& stopToken)
{
    std::unique_lock lock(mutex);

    if (preserveOrder)
    {
        completedArrived.wait(lock, stopToken, [this] { return completed.contains(nextToEmit) || inputExhausted || failure; });
        if (const auto it = completed.find(nextToEmit); it != completed.end())
        {
            auto processed = std::move(it->second);
            completed.erase(it);
            ++nextToEmit;
            return processed;
        }
        return std::nullopt;
    }

    completedArrived.wait(lock, stopToken, [this] { return not completed.empty() || inputExhausted || failure; });
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
    /// Before anything else, so a failure is not mistaken for end of stream and does not come out
    /// as a short result either.
    rethrowIfFailed();

    if (!partiallyEmitted.has_value())
    {
        partiallyEmitted = takeNextCompleted(stopToken);
    }
    if (!partiallyEmitted.has_value())
    {
        /// A worker may have failed while we waited, which also releases the wait above.
        rethrowIfFailed();
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
    /// Tell the producer first: from here on nothing reads this channel, so its sink must stop
    /// retrying, and the buffers parked in the channel can go back to the pool now. Null only if
    /// open() never got as far as attaching.
    if (channel != nullptr)
    {
        channel->closeConsumer();
    }
    workerStop.request_stop();
    /// Thread's destructor joins, so clearing waits for every worker to leave its loop, and
    /// resetting does the same for the intake thread.
    workers.clear();
    intake.reset();
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
