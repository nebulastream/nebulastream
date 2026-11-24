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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <expected>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Async/HandoffChannel.hpp>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/RuntimeValueNautilus.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncOperator.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/PreparedAsyncCall.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp> /// NOLINT(misc-include-cleaner) Public Asio co_spawn provider; checker misattributes completion overload
#include <boost/asio/error.hpp> /// NOLINT(misc-include-cleaner) Public Asio error provider; used through operation error constants
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp> /// NOLINT(misc-include-cleaner) Public Boost.System provider; checker misses direct use
#include <nautilus/val.hpp> /// NOLINT(misc-include-cleaner) Installed public Nautilus val provider; checker misattributes val uses
#include <AsyncGraphExecution.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Sizes and counts arrive as signed integer literals.
std::expected<size_t, Exception> sizeFromLiteral(const ConfigLiteral& literal)
{
    return NES::tryGetOr<int64_t>(literal, expectedType<size_t>()).and_then(narrowConfigValue<int64_t, size_t>);
}

/// NOLINTBEGIN(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<size_t> MAX_CONCURRENCY{
    Identifier::parse("MAX_CONCURRENCY"),
    "Rows whose calls are in flight at the same time",
    sizeFromLiteral,
    AsyncSourceConfig::DEFAULT_MAX_CONCURRENCY};

const ConfigField<size_t> MAX_RETRIES{
    Identifier::parse("MAX_RETRIES"),
    "Further attempts after retryable async errors, with exponential backoff starting at 100 ms",
    sizeFromLiteral,
    AsyncSourceConfig::DEFAULT_MAX_RETRIES};

/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

/// All Async sources share one event-loop thread; source threads wait for results.
/// Per-source strands serialize coroutine execution.
class SharedEventLoop
{
public:
    static boost::asio::io_context& context()
    {
        static SharedEventLoop loop;
        return loop.ioContext;
    }

    SharedEventLoop(const SharedEventLoop&) = delete;
    SharedEventLoop& operator=(const SharedEventLoop&) = delete;
    SharedEventLoop(SharedEventLoop&&) = delete;
    SharedEventLoop& operator=(SharedEventLoop&&) = delete;

private:
    SharedEventLoop() : work(boost::asio::make_work_guard(ioContext)), thread([this] { ioContext.run(); }) { }

    ~SharedEventLoop()
    {
        work.reset();
        ioContext.stop();
    }

    boost::asio::io_context ioContext;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work;
    /// Declared last: joined before the context it runs is destroyed.
    std::jthread thread;
};

constexpr std::chrono::milliseconds RETRY_BACKOFF{100};
/// Caps the backoff at 100 ms * 2^10, about 100 s.
constexpr size_t MAX_BACKOFF_DOUBLINGS = 10;
}

Schema<QualifiedErasedConfigField, Ordered> AsyncSource::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("ASYNC_SOURCE"), MAX_CONCURRENCY, MAX_RETRIES);
}

std::expected<AsyncSourceConfig, Exception> AsyncSourceConfig::fromConfig(const InstantiatedConfig&)
{
    PRECONDITION(
        false, "Async sources require an AsyncCallGraph supplied by the optimizer and cannot be instantiated from literal configuration");
    std::unreachable();
}

AsyncSource::AsyncSource(const AsyncSourceConfig& config, const SourceDescriptor& sourceDescriptor)
    : AsyncSource(
          config,
          sourceDescriptor,
          config.channel ? config.channel->takeReceiver() : throw InvalidConfigParameter("AsyncSource requires a non-null channel"))
{
}

/// Failure to drain is fatal: pending callbacks may still borrow this source and its buffers.
/// NOLINTNEXTLINE(bugprone-exception-escape) cleanup scheduling may allocate; destruction must not continue on failure
AsyncSource::~AsyncSource()
{
    cancelAndDrain();
    channel.close();
}

AsyncSource::AsyncSource(const AsyncSourceConfig& config, const SourceDescriptor& sourceDescriptor, HandoffChannel::Receiver receiver)
    : maxConcurrency(std::max<size_t>(1, config.maxConcurrency))
    , maxRetries(config.maxRetries)
    , layout(config.calls, sourceDescriptor.getSchema())
    , graph(std::make_shared<const AsyncCallGraph>(config.calls))
    , calls(createCallStates(*graph))
    , outputMappings(graph->getOutputs())
    , channel(std::move(receiver))
    , strand(boost::asio::make_strand(SharedEventLoop::context()))
{
    for (const auto& entry : calls)
    {
        const auto& call = entry.second;
        if (const auto operatorConcurrency = call.asyncOperator.maxConcurrency())
        {
            maxConcurrency = std::min(maxConcurrency, std::max<size_t>(1, *operatorConcurrency));
        }
    }
    for (auto& entry : calls)
    {
        auto& contexts = entry.second.contexts;
        contexts.reserve(maxConcurrency);
        const AsyncExecutionContext operatorContext;
        for (size_t slot = 0; slot < maxConcurrency; ++slot)
        {
            contexts.push_back(operatorContext.createSlotContext());
        }
    }
    cancelSignals = std::vector<boost::asio::cancellation_signal>(maxConcurrency);
    executions.resize(maxConcurrency);
    for (size_t slot = maxConcurrency; slot > 0; --slot)
    {
        freeSlots.push_back(slot - 1);
    }
}

std::unordered_map<AsyncCall::Ptr, AsyncSource::CallState> AsyncSource::createCallStates(const AsyncCallGraph& graph)
{
    std::unordered_map<AsyncCall::Ptr, CallState> states;
    for (const auto& node : graph.getCalls())
    {
        states.emplace(node, CallState{.asyncOperator = node->getAsyncOperator(), .contexts = {}});
    }
    return states;
}

void AsyncSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    rowBufferRef = LowerSchemaProvider::lowerSchema(bufferProvider->getBufferSize(), layout.getSchema(), MemoryLayoutType::ROW_LAYOUT);
    channel.setListener(
        [this]
        {
            {
                const std::scoped_lock lock(inFlight.mutex);
                inFlight.channelChanged = true;
            }
            inFlight.changed.notify_all();
        });
}

void AsyncSource::close()
{
    /// Also after a failure or a stop request, which already drained: rows of this source must not outlive it.
    cancelAndDrain();
    channel.close();
}

RuntimeValue
AsyncSource::readInput(const TupleBufferRef& layout, const InputFieldReference& field, const TupleBuffer& input, const size_t row)
{
    /// NOLINTBEGIN(misc-include-cleaner) nautilus::val is declared by public nautilus/val.hpp.
    const RecordBuffer buffer{BorrowedNautilusBuffer::from(nautilus::val<const TupleBuffer*>{std::addressof(input)})};
    /// NOLINTEND(misc-include-cleaner)
    /// NOLINTNEXTLINE(misc-include-cleaner) nautilus::val is declared by public nautilus/val.hpp.
    auto index = nautilus::val<uint64_t>{row};
    const auto record = layout.readRecord({field.getField()}, buffer, index);
    return fromVarVal(record.read(field.getField()), field.getDataType());
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<AsyncResult<SharedRuntimeValues>>
AsyncSource::callWithRetries(AsyncCall::Ptr node, const size_t slot, AsyncRowHandle row)
{
    if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        co_return std::unexpected(AsyncError::cancelled());
    }
    auto& call = calls.at(node);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) slot comes from the bounded free-slot pool
    auto preparedResult = call.asyncOperator.prepare(call.contexts[slot], row);
    if (!preparedResult)
    {
        co_return std::unexpected(std::move(preparedResult.error()));
    }
    auto prepared = std::move(*preparedResult);
    PRECONDITION(prepared, "Async operator preparation must return an invocation");
    if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        co_return std::unexpected(AsyncError::cancelled());
    }
    const auto retryLimit = prepared->getRetryOverride().value_or(maxRetries);
    for (size_t attempt = 0;; ++attempt)
    {
        /// Result shape/type invariants are checked by the graph, outside retries.
        auto outcome = co_await prepared->executeAttempt();
        if (outcome)
        {
            co_return outcome;
        }
        if (outcome.error().kind != AsyncError::Type::Retryable || attempt >= retryLimit)
        {
            co_return outcome;
        }
        if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
        {
            co_return std::unexpected(AsyncError::cancelled());
        }
        const auto backoff = RETRY_BACKOFF * (size_t{1} << std::min<size_t>(attempt, MAX_BACKOFF_DOUBLINGS));
        NES_WARNING(
            "Async call of {} failed, attempt {} of {} follows in {} ms",
            node->getFunctionType(),
            attempt + 2,
            retryLimit + 1,
            backoff.count());
        const bool throwIfCancelled = co_await boost::asio::this_coro::throw_if_cancelled();
        co_await boost::asio::this_coro::throw_if_cancelled(false);
        boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, backoff};
        boost::system::error_code waitError; /// NOLINT(misc-include-cleaner) error_code is declared by public boost/system/error_code.hpp.
        co_await timer.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, waitError));
        const bool wasCancelled = (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none;
        co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
        if (wasCancelled)
        {
            co_return std::unexpected(AsyncError::cancelled());
        }
        if (waitError)
        {
            co_return std::unexpected(AsyncError{.exception = IOError(waitError.message()), .kind = AsyncError::Type::Permanent});
        }
    }
}

/// The in-flight state retains this deque element until finishRow marks its last row done, or shutdown drains all rows.
boost::asio::awaitable<AsyncResult<void>> AsyncSource::processRow(const size_t slot, InFlightBuffer& buffer, const size_t row)
{
    if (stopping)
    {
        co_return std::unexpected(AsyncError::cancelled());
    }
    auto execution = std::make_shared<AsyncGraphExecution>(
        strand,
        graph,
        [layout = rowBufferRef, input = buffer.input, row](const InputFieldReference& field)
        { return readInput(*layout, field, input, row); },
        [this, slot](AsyncCall::Ptr node, AsyncRowHandle input) { return callWithRetries(std::move(node), slot, std::move(input)); });
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) slot comes from the bounded free-slot pool
    executions[slot] = execution;
    auto outcome = co_await execution->run();
    if (!outcome)
    {
        co_return std::unexpected(std::move(outcome.error()));
    }
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) row is admitted within the sized results array
    buffer.results[row] = std::move(*outcome);
    co_return AsyncResult<void>{};
}

void AsyncSource::startRows()
{
    bool started = false;
    while (!stopping && !freeSlots.empty())
    {
        std::unique_lock lock(inFlight.mutex);
        const auto next = std::ranges::find_if(inFlight.buffers, [](const auto& candidate) { return candidate.nextRow < candidate.rows; });
        if (inFlight.failure || inFlight.infrastructureFailure || next == inFlight.buffers.end())
        {
            break;
        }
        auto& buffer = *next;
        const auto row = buffer.nextRow++;
        --inFlight.unstartedRows;
        ++inFlight.runningRows;
        lock.unlock();
        started = true;
        const auto slot = freeSlots.back();
        freeSlots.pop_back();
        /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) slot indexes equally sized per-slot storage
        boost::asio::co_spawn( /// NOLINT(misc-include-cleaner) co_spawn is declared by public boost/asio/co_spawn.hpp.
            strand,
            processRow(slot, buffer, row),
            boost::asio::bind_cancellation_slot(
                cancelSignals[slot].slot(),
                [this, &buffer, slot](const std::exception_ptr& error, AsyncResult<void> result)
                {
                    if (!error)
                    {
                        finishRow(buffer, slot, error, result);
                        return;
                    }
                    {
                        const std::scoped_lock lock(inFlight.mutex);
                        if (!stopping && !inFlight.failure && !inFlight.infrastructureFailure)
                        {
                            inFlight.infrastructureFailure = error;
                        }
                    }
                    inFlight.changed.notify_all();
                    const auto execution = executions[slot];
                    if (!execution)
                    {
                        finishRow(buffer, slot, error, result);
                        return;
                    }
                    boost::asio::co_spawn( /// NOLINT(misc-include-cleaner) co_spawn is declared by public boost/asio/co_spawn.hpp.
                        strand,
                        execution->cancelAndDrain(),
                        boost::asio::bind_cancellation_slot(
                            boost::asio::cancellation_slot{},
                            [this, &buffer, slot, error](std::exception_ptr) { finishRow(buffer, slot, error, AsyncResult<void>{}); }));
                }));
        /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    }
    if (started)
    {
        /// Fewer rows wait to start, which may let the source thread take the next buffer.
        inFlight.changed.notify_all();
    }
}

void AsyncSource::finishRow(
    InFlightBuffer& buffer, const size_t slot, const std::exception_ptr& infrastructureError, const AsyncResult<void>& outcome)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) completion retains its admitted slot
    executions[slot].reset();
    freeSlots.push_back(slot);
    {
        const std::scoped_lock lock(inFlight.mutex);
        --inFlight.runningRows;
        /// A row cancelled because the query stops did not fail.
        if (!stopping && !inFlight.failure && !inFlight.infrastructureFailure)
        {
            if (infrastructureError)
            {
                inFlight.infrastructureFailure = infrastructureError;
            }
            else if (!outcome)
            {
                inFlight.failure = outcome.error();
            }
        }
        if (--buffer.unfinishedRows == 0)
        {
            buffer.done = true;
        }
        /// No buffer access after releasing this lock: the source may now move it out or clear it during shutdown.
    }
    inFlight.changed.notify_all();
    startRows();
}

void AsyncSource::admit(TupleBuffer input)
{
    const auto rows = input.getNumberOfTuples();
    InFlightBuffer buffer{
        .input = std::move(input),
        .rows = rows,
        .nextRow = 0,
        .unfinishedRows = rows,
        .results = std::vector<std::optional<SharedRuntimeValues>>(rows),
        .done = rows == 0};
    {
        const std::scoped_lock lock(inFlight.mutex);
        inFlight.buffers.push_back(std::move(buffer));
        inFlight.unstartedRows += rows;
    }
    boost::asio::post(strand, [this] { startRows(); });
}

void AsyncSource::writeOutput(const InFlightBuffer& buffer, TupleBuffer& output) const
{
    auto input = buffer.input;
    output.setSequenceNumber(input.getSequenceNumber());
    output.setChunkNumber(input.getChunkNumber());
    output.setLastChunk(input.isLastChunk());
    output.setOriginId(input.getOriginId());
    output.setWatermark(input.getWatermark());
    output.setCreationTimestampInMS(input.getCreationTimestampInMS());
    /// Stored in the same order, so the child indices in the copied VARSIZED fields stay valid.
    for (uint32_t child = 0; child < input.getNumberOfChildBuffers(); ++child)
    {
        auto childBuffer = input.loadChildBuffer(ChildBufferIndex{child});
        [[maybe_unused]] const auto index = output.storeChildBuffer(childBuffer);
    }

    const auto rowSize = layout.getRowSize();
    PRECONDITION(buffer.rows <= output.getBufferSize() / rowSize, "handed-off buffer holds more rows than the output buffer");
    const auto inputBytes = input.getAvailableMemoryArea();
    const auto outputBytes = output.getAvailableMemoryArea();
    for (size_t row = 0; row < buffer.rows; ++row)
    {
        for (const auto& [offset, size] : layout.getCopiedFields())
        {
            const auto position = (row * rowSize) + offset;
            std::memcpy(outputBytes.subspan(position).data(), inputBytes.subspan(position).data(), size);
        }
    }
    writeResults(buffer, output);
}

void AsyncSource::writeResults(const InFlightBuffer& buffer, TupleBuffer& output) const
{
    PRECONDITION(buffer.rows <= output.getBufferSize() / layout.getRowSize(), "async output rows must fit the buffer");
    for (size_t row = 0; row < buffer.rows; ++row)
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) results is sized to buffer.rows
        const auto& rowResult = buffer.results[row];
        INVARIANT(rowResult.has_value(), "A completed async row must have a result");
        Record results;
        for (size_t position = 0; position < outputMappings.size(); ++position)
        {
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access) completed results match graph output shape
            results.write(outputMappings[position].field, toVarVal((*rowResult)[position]));
        }
        const RecordBuffer recordBuffer{BorrowedNautilusBuffer::from(nautilus::val<TupleBuffer*>{std::addressof(output)})};
        auto index = nautilus::val<uint64_t>{row};
        const auto written
            = rowBufferRef->writeRecord(index, recordBuffer, results, nautilus::val<AbstractBufferProvider*>{bufferProvider.get()});
        INVARIANT(static_cast<bool>(written.successful), "async output row must fit the output buffer");
    }
}

void AsyncSource::cancelAndDrain()
{
    boost::asio::post(
        strand,
        [this]
        {
            stopping = true;
            for (const auto& execution : executions)
            {
                if (execution)
                {
                    execution->cancel();
                }
            }
            for (auto& signal : cancelSignals)
            {
                signal.emit(boost::asio::cancellation_type::terminal);
            }
        });
    {
        std::unique_lock lock(inFlight.mutex);
        inFlight.changed.wait(lock, [this] { return inFlight.runningRows == 0; });
        inFlight.buffers.clear();
        inFlight.unstartedRows = 0;
    }
    /// The strand runs handlers in the order they were posted: once this one ran, no handler of this source is left.
    std::promise<void> drained;
    boost::asio::post(strand, [&drained] { drained.set_value(); });
    drained.get_future().wait();
}

Source::FillTupleBufferResult AsyncSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    std::unique_lock lock(inFlight.mutex);
    while (true)
    {
        if (inFlight.failure || inFlight.infrastructureFailure)
        {
            auto failure = inFlight.failure;
            const auto infrastructureFailure = inFlight.infrastructureFailure;
            lock.unlock();
            cancelAndDrain();
            if (infrastructureFailure)
            {
                std::rethrow_exception(infrastructureFailure);
            }
            INVARIANT(failure.has_value(), "A failed async source must retain its operational error");
            throw std::move(failure->exception);
        }
        if (!inFlight.buffers.empty() && inFlight.buffers.front().done)
        {
            auto buffer = std::move(inFlight.buffers.front());
            inFlight.buffers.pop_front();
            lock.unlock();
            /// All row callbacks have finished before done is published. Do not wait for other owners:
            /// a fanout consumer may retain the original buffer indefinitely or read the result slots.
            if (buffer.input.getReferenceCounter() == 1)
            {
                writeResults(buffer, buffer.input);
                tupleBuffer = std::move(buffer.input);
            }
            else
            {
                if (!tupleBuffer || tupleBuffer.getAvailableMemoryArea().data() == buffer.input.getAvailableMemoryArea().data())
                {
                    /// A copy must not wait for a pooled buffer while retaining an input from that same
                    /// exhausted pool. Fall back to unpooled memory, or fail instead of deadlocking.
                    auto output = bufferProvider->getBufferNoBlocking();
                    if (!output)
                    {
                        output = bufferProvider->getUnpooledBuffer(bufferProvider->getBufferSize());
                    }
                    if (!output)
                    {
                        throw CannotAllocateBuffer("Cannot allocate async output buffer");
                    }
                    tupleBuffer = std::move(*output);
                }
                writeOutput(buffer, tupleBuffer);
            }
            return FillTupleBufferResult::withBytes(buffer.rows);
        }
        if (inputExhausted && inFlight.buffers.empty())
        {
            return FillTupleBufferResult::eos();
        }

        /// The next buffer is taken once the rows waiting to start would no longer keep every slot busy, so the calls
        /// run across buffer boundaries while the channel, not this source, holds what the calls cannot keep up with.
        const bool wantsInput = !inputExhausted && inFlight.buffers.size() < maxConcurrency && inFlight.unstartedRows < maxConcurrency;
        if (wantsInput)
        {
            const bool idle = inFlight.buffers.empty();
            inFlight.channelChanged = false;
            lock.unlock();
            /// With nothing in flight, the next buffer is all there is to wait for.
            auto input = idle ? channel.pop(stopToken) : channel.tryPop();
            if (stopToken.stop_requested())
            {
                cancelAndDrain();
                return FillTupleBufferResult::eos();
            }
            if (input)
            {
                admit(std::move(*input));
            }
            else
            {
                inputExhausted = channel.exhausted();
            }
            lock.lock();
            if (input || inputExhausted)
            {
                continue;
            }
        }
        const auto progressed = [&]
        {
            return inFlight.failure || inFlight.infrastructureFailure || (!inFlight.buffers.empty() && inFlight.buffers.front().done)
                || (wantsInput && inFlight.channelChanged);
        };
        if (!inFlight.changed.wait(lock, stopToken, progressed))
        {
            lock.unlock();
            cancelAndDrain();
            return FillTupleBufferResult::eos();
        }
    }
}

std::ostream& AsyncSource::toString(std::ostream& str) const
{
    return str << "AsyncSource";
}

}
