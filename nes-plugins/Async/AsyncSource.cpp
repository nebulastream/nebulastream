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
#include <ranges>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <DataTypes/RuntimeValueNautilus.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
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
#include <Sources/AsyncFunction.hpp>
#include <Sources/AsyncSourceConfig.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <Util/Variant.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/system_error.hpp>
#include <fmt/format.h>
#include <AsyncFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <HandoffChannel.hpp>

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
const ConfigField<AsyncChannelId> CHANNEL{
    Identifier::parse("CHANNEL"),
    "UUID of the channel the Handoff sink of the same stage hands its buffers to",
    [](const ConfigLiteral& literal)
    {
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](const std::string& value) -> std::expected<AsyncChannelId, Exception>
                {
                    const auto uuid = stringToUUID(value);
                    if (!uuid)
                    {
                        return std::unexpected{InvalidConfigParameter("AsyncSource: channel must be a valid UUID, got: {}", value)};
                    }
                    return AsyncChannelId{*uuid};
                });
    }};

const ConfigField<size_t> MAX_CONCURRENCY{
    Identifier::parse("MAX_CONCURRENCY"),
    "Rows whose calls are in flight at the same time",
    sizeFromLiteral,
    AsyncSourceConfig::DEFAULT_MAX_CONCURRENCY};

const ConfigField<size_t> CHANNEL_CAPACITY{
    Identifier::parse("CHANNEL_CAPACITY"),
    "Buffers the channel holds, if this source creates it",
    sizeFromLiteral,
    AsyncSourceConfig::DEFAULT_CHANNEL_CAPACITY};

const ConfigField<size_t> MAX_RETRIES{
    Identifier::parse("MAX_RETRIES"),
    "Further attempts of a failed call, the n-th after 100 ms * 2^n",
    sizeFromLiteral,
    AsyncSourceConfig::DEFAULT_MAX_RETRIES};

/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

/// One event loop for all Async sources: calls need no dedicated threads; ordinary source threads wait for their results.
/// One loop thread; add threads if completions get expensive, the per-source strands keep that safe
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
    return createConfigSchema(Identifier::parse("ASYNC_SOURCE"), CHANNEL, MAX_CONCURRENCY, CHANNEL_CAPACITY, MAX_RETRIES);
}

std::expected<AsyncSourceConfig, Exception> AsyncSourceConfig::fromConfig(const InstantiatedConfig&)
{
    return std::unexpected{InvalidConfigParameter("Async sources require a planner-supplied AsyncCallGraph, not literal configuration")};
}

AsyncSource::AsyncSource(const AsyncSourceConfig& config, const SourceDescriptor& sourceDescriptor)
    : channelId(config.channel)
    , maxConcurrency(std::max<size_t>(1, config.maxConcurrency))
    , maxRetries(config.maxRetries)
    , graph(std::make_shared<const AsyncCallGraph>(config.calls))
    , channel(config.channel, config.channelCapacity, HandoffChannel::Role::SOURCE)
    , strand(boost::asio::make_strand(SharedEventLoop::context()))
{
    static_assert(AsyncSourceConfig::DEFAULT_CHANNEL_CAPACITY == HandoffChannel::DEFAULT_CAPACITY);

    outputSchema = sourceDescriptor.getSchema();
    /// Only unchanged fields are copied as raw slots. Typed argument/result access goes through TupleBufferRef.
    std::unordered_map<std::string, std::pair<size_t, DataType>> outputFields;
    for (const auto& field : sourceDescriptor.getSchema())
    {
        outputFields.emplace(fmt::format("{}", field.getFullyQualifiedName()), std::pair{outputRowSize, field.getDataType()});
        outputRowSize += field.getDataType().getSizeInBytesWithNull();
    }
    std::unordered_map<std::string, size_t> inputOffsets;
    inputSchema = graph->getInputSchema();
    for (const auto& field : inputSchema)
    {
        const auto name = fmt::format("{}", field.getFullyQualifiedName());
        const auto output = outputFields.find(name);
        if (output == outputFields.end())
        {
            throw CannotOpenSource("handed-off field {} is not produced by the async source", name);
        }
        INVARIANT(
            field.getDataType() == output->second.second,
            "async input field has type {}, expected {}",
            field.getDataType(),
            output->second.second);
        if (!inputOffsets.emplace(name, inputRowSize).second)
        {
            throw CannotOpenSource("duplicate handed-off field {}", name);
        }
        inputRowSize += field.getDataType().getSizeInBytesWithNull();
    }
    if (inputOffsets.size() != outputFields.size())
    {
        throw CannotOpenSource("handed-off rows must have exactly the async source's fields");
    }
    outputValues = graph->getOutputs();
    IF_INVARIANT(for (const auto& output : outputValues) {
        INVARIANT(
            outputFields.at(fmt::format("{}", output.field)).second == output.result.getDataType(),
            "async output field {} does not match declared result type {}",
            output.field,
            output.result.getDataType());
    })
    for (const auto& node : graph->getCalls())
    {
        BoundCall call{.binding = {.argumentTypes = {}, .constantArguments = {}, .resultTypes = node->getResultTypes()}, .instances = {}};
        for (const auto& argument : node->getArguments())
        {
            call.binding.argumentTypes.push_back(std::visit([](const auto& value) { return value.getDataType(); }, argument));
            if (const auto* constant = std::get_if<RuntimeValue>(&argument))
            {
                call.binding.constantArguments.emplace_back(*constant);
            }
            else
            {
                call.binding.constantArguments.emplace_back(std::nullopt);
            }
        }
        const auto factory = AsyncFunctionRegistry::instance().find(node->getFunctionType());
        if (!factory)
        {
            throw UnknownFunctionType("no async implementation registered for {}", node->getFunctionType());
        }
        call.instances.push_back((*factory)(AsyncFunctionRegistryArguments{.binding = call.binding}));
        maxConcurrency = std::min(maxConcurrency, std::max<size_t>(1, call.instances.front()->maxConcurrency().value_or(maxConcurrency)));
        calls.emplace(node, std::move(call));
    }
    for (auto& [node, call] : calls)
    {
        const auto factory = AsyncFunctionRegistry::instance().find(node->getFunctionType());
        while (call.instances.size() < maxConcurrency)
        {
            call.instances.push_back((*factory)(AsyncFunctionRegistryArguments{.binding = call.binding}));
        }
    }
    cancelSignals = std::vector<boost::asio::cancellation_signal>(maxConcurrency);
    executions.resize(maxConcurrency);
    for (size_t slot = maxConcurrency; slot > 0; --slot)
    {
        freeSlots.push_back(slot - 1);
    }

    for (const auto& [name, inputOffset] : inputOffsets)
    {
        if (std::ranges::none_of(outputValues, [&](const auto& output) { return fmt::format("{}", output.field) == name; }))
        {
            const auto& [outputOffset, type] = outputFields.at(name);
            copiedFields.push_back({.inputOffset = inputOffset, .outputOffset = outputOffset, .size = type.getSizeInBytesWithNull()});
        }
    }
}

void AsyncSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    inputBufferRef = LowerSchemaProvider::lowerSchema(bufferProvider->getBufferSize(), inputSchema, MemoryLayoutType::ROW_LAYOUT);
    outputBufferRef = LowerSchemaProvider::lowerSchema(bufferProvider->getBufferSize(), outputSchema, MemoryLayoutType::ROW_LAYOUT);
    channel->setListener(
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
    channel.release();
}

RuntimeValue AsyncSource::readInput(const InputFieldReference& field, const TupleBuffer& input, const size_t row) const
{
    const RecordBuffer buffer{BorrowedNautilusBuffer::from(nautilus::val<const TupleBuffer*>{std::addressof(input)})};
    auto index = nautilus::val<uint64_t>{row};
    const auto record = inputBufferRef->readRecord({field.getField()}, buffer, index);
    return fromVarVal(record.read(field.getField()), field.getDataType());
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<AsyncValues> AsyncSource::callWithRetries(AsyncCall::Ptr node, const size_t slot, AsyncValues arguments) const
{
    const auto& call = calls.at(node);
    auto& function = *call.instances[slot];
    const auto retryLimit = function.maxRetries(arguments).value_or(maxRetries);
    for (size_t attempt = 0;; ++attempt)
    {
        std::optional<AsyncCallResult> result;
        std::exception_ptr failure;
        try
        {
            result = co_await function.call(arguments);
        }
        /// The failure is kept and rethrown unchanged; CPPTRACE_TRY cannot wrap a co_await.
        catch (...) /// NOLINT(no-raw-catch-all)
        {
            failure = std::current_exception();
        }
        if (result)
        {
            /// The graph checks result invariants outside this retry loop; shape/type bugs are never retried.
            co_return std::move(*result);
        }
        if (attempt >= retryLimit
            || (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
        {
            std::rethrow_exception(failure);
        }
        const auto backoff = RETRY_BACKOFF * (size_t{1} << std::min<size_t>(attempt, MAX_BACKOFF_DOUBLINGS));
        NES_WARNING(
            "Async call of {} failed, attempt {} of {} follows in {} ms",
            node->getFunctionType(),
            attempt + 2,
            retryLimit + 1,
            backoff.count());
        boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, backoff};
        co_await timer.async_wait(boost::asio::use_awaitable);
    }
}

/// The in-flight state retains this deque element until finishRow marks its last row done, or shutdown drains all rows.
boost::asio::awaitable<void> AsyncSource::processRow(const size_t slot, InFlightBuffer& buffer, const size_t row)
{
    if (stopping)
    {
        throw boost::system::system_error{boost::asio::error::operation_aborted};
    }
    auto execution = std::make_shared<AsyncGraphExecution>(
        strand,
        graph,
        [this, &buffer, row](const InputFieldReference& field) { return readInput(field, buffer.input, row); },
        [this, slot](AsyncCall::Ptr node, AsyncValues arguments) { return callWithRetries(std::move(node), slot, std::move(arguments)); });
    executions[slot] = execution;
    buffer.results[row] = co_await execution->run();
}

void AsyncSource::startRows()
{
    bool started = false;
    while (!stopping && !freeSlots.empty())
    {
        std::unique_lock lock(inFlight.mutex);
        const auto next = std::ranges::find_if(inFlight.buffers, [](const auto& candidate) { return candidate.nextRow < candidate.rows; });
        if (inFlight.failure || next == inFlight.buffers.end())
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
        boost::asio::co_spawn(
            strand,
            processRow(slot, buffer, row),
            boost::asio::bind_cancellation_slot(
                cancelSignals[slot].slot(), [this, &buffer, slot](const std::exception_ptr& error) { finishRow(buffer, slot, error); }));
    }
    if (started)
    {
        /// Fewer rows wait to start, which may let the source thread take the next buffer.
        inFlight.changed.notify_all();
    }
}

void AsyncSource::finishRow(InFlightBuffer& buffer, const size_t slot, const std::exception_ptr& error)
{
    executions[slot].reset();
    freeSlots.push_back(slot);
    {
        const std::scoped_lock lock(inFlight.mutex);
        --inFlight.runningRows;
        /// A row cancelled because the query stops did not fail.
        if (error && !stopping && !inFlight.failure)
        {
            inFlight.failure = error;
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
        .results = std::vector<std::optional<AsyncValues>>(rows),
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

    PRECONDITION(buffer.rows * outputRowSize <= output.getBufferSize(), "handed-off buffer holds more rows than the output buffer");
    const auto in = input.getAvailableMemoryArea();
    const auto out = output.getAvailableMemoryArea();
    for (size_t row = 0; row < buffer.rows; ++row)
    {
        for (const auto& [inputOffset, outputOffset, size] : copiedFields)
        {
            std::memcpy(
                out.subspan((row * outputRowSize) + outputOffset).data(), in.subspan((row * inputRowSize) + inputOffset).data(), size);
        }
        INVARIANT(buffer.results[row].has_value(), "A completed async row must have a result");
        Record results;
        for (size_t position = 0; position < outputValues.size(); ++position)
        {
            results.write(outputValues[position].field, toVarVal((*buffer.results[row])[position]));
        }
        const RecordBuffer recordBuffer{BorrowedNautilusBuffer::from(nautilus::val<TupleBuffer*>{std::addressof(output)})};
        auto index = nautilus::val<uint64_t>{row};
        const auto written
            = outputBufferRef->writeRecord(index, recordBuffer, results, nautilus::val<AbstractBufferProvider*>{bufferProvider.get()});
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
        if (inFlight.failure)
        {
            const auto failure = inFlight.failure;
            lock.unlock();
            cancelAndDrain();
            std::rethrow_exception(failure);
        }
        if (!inFlight.buffers.empty() && inFlight.buffers.front().done)
        {
            const auto buffer = std::move(inFlight.buffers.front());
            inFlight.buffers.pop_front();
            lock.unlock();
            writeOutput(buffer, tupleBuffer);
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
            auto input = idle ? channel->pop(stopToken) : channel->tryPop();
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
                inputExhausted = channel->exhausted();
            }
            lock.lock();
            if (input || inputExhausted)
            {
                continue;
            }
        }
        const auto progressed = [&]
        {
            return inFlight.failure || (!inFlight.buffers.empty() && inFlight.buffers.front().done)
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
    return str << "AsyncSource(" << channelId << ")";
}

}
