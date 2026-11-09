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
#include <Sources/AsyncFunction.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <Util/UUID.hpp>
#include <Util/Variant.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <fmt/format.h>
#include <rfl/Generic.hpp>
#include <rfl/json/read.hpp>
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
const ConfigField<std::string> CHANNEL{
    Identifier::parse("CHANNEL"),
    "UUID of the channel the Handoff sink of the same stage hands its buffers to",
    [](const ConfigLiteral& literal)
    {
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
            .and_then(
                [](const std::string& value) -> std::expected<std::string, Exception>
                {
                    if (!stringToUUID(value))
                    {
                        return std::unexpected{InvalidConfigParameter("AsyncSource: channel must be a valid UUID, got: {}", value)};
                    }
                    return value;
                });
    }};

const ConfigField<std::string> CALLS{Identifier::parse("CALLS"), "JSON list of the calls of the stage, see AsyncSourceConfig::calls"};

const ConfigField<std::string> INPUT_FIELDS{Identifier::parse("INPUT_FIELDS"), "Comma-separated field order of the handed-off rows"};

const ConfigField<size_t> MAX_CONCURRENCY{
    Identifier::parse("MAX_CONCURRENCY"), "Rows whose calls are in flight at the same time", sizeFromLiteral, size_t{16}};

const ConfigField<size_t> CHANNEL_CAPACITY{
    Identifier::parse("CHANNEL_CAPACITY"),
    "Buffers the channel holds, if this source creates it",
    sizeFromLiteral,
    HandoffChannel::DEFAULT_CAPACITY};

const ConfigField<size_t> MAX_RETRIES{
    Identifier::parse("MAX_RETRIES"), "Further attempts of a failed call, the n-th after 100 ms * 2^n", sizeFromLiteral, size_t{0}};

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

void requireSameType(const DataType actual, const DataType expected, const std::string_view what)
{
    if (actual != expected)
    {
        throw CannotOpenSource("async {} has type {}, expected {}", what, actual, expected);
    }
}
}

Schema<QualifiedErasedConfigField, Ordered> AsyncSource::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("ASYNC_SOURCE"), CHANNEL, CALLS, INPUT_FIELDS, MAX_CONCURRENCY, CHANNEL_CAPACITY, MAX_RETRIES);
}

std::expected<AsyncSourceConfig, Exception> AsyncSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return AsyncSourceConfig{
        .channel = config.get(CHANNEL),
        .calls = config.get(CALLS),
        .inputFields = config.get(INPUT_FIELDS),
        .maxConcurrency = config.get(MAX_CONCURRENCY),
        .channelCapacity = config.get(CHANNEL_CAPACITY),
        .maxRetries = config.get(MAX_RETRIES)};
}

AsyncSource::AsyncSource(const AsyncSourceConfig& config, const SourceDescriptor& sourceDescriptor)
    : channelId(config.channel)
    , maxConcurrency(std::max<size_t>(1, config.maxConcurrency))
    , maxRetries(config.maxRetries)
    , channel(config.channel, config.channelCapacity, HandoffChannel::Role::SOURCE)
    , strand(boost::asio::make_strand(SharedEventLoop::context()))
{
    const auto parsed = rfl::json::read<rfl::Generic>(config.calls);
    if (!parsed)
    {
        throw CannotOpenSource("malformed CALLS of async source: {}", parsed.error().what());
    }
    const auto descriptions = ReflectionContext{}.unreflect<std::vector<AsyncCall>>(*parsed);

    outputSchema = sourceDescriptor.getSchema();
    /// Only unchanged fields are copied as raw slots. Typed argument/result access goes through TupleBufferRef.
    std::unordered_map<std::string, std::pair<size_t, DataType>> outputFields;
    for (const auto& field : sourceDescriptor.getSchema())
    {
        outputFields.emplace(fmt::format("{}", field.getFullyQualifiedName()), std::pair{outputRowSize, field.getDataType()});
        outputRowSize += field.getDataType().getSizeInBytesWithNull();
    }
    std::unordered_map<std::string, size_t> inputOffsets;
    std::vector<QualifiedUnboundField> inputFields;
    for (const auto& name : config.inputFields | std::views::split(',') | std::ranges::to<std::vector<std::string>>())
    {
        const auto field = outputFields.find(name);
        if (field == outputFields.end())
        {
            throw CannotOpenSource("handed-off field {} is not produced by the async source", name);
        }
        if (!inputOffsets.emplace(name, inputRowSize).second)
        {
            throw CannotOpenSource("duplicate handed-off field {}", name);
        }
        inputFields.emplace_back(QualifiedIdentifier::create(Identifier::parse(name)), field->second.second);
        inputRowSize += field->second.second.getSizeInBytesWithNull();
    }
    if (inputOffsets.size() != outputFields.size())
    {
        throw CannotOpenSource("handed-off rows must have exactly the async source's fields");
    }
    inputSchema = Schema<QualifiedUnboundField, Ordered>{std::move(inputFields)};

    for (const auto& [outputs, function, resultTypes, arguments] : descriptions)
    {
        BoundCall call{.function = function, .binding = {}, .arguments = {}, .inputFields = {}, .level = 0, .instances = {}};
        for (const auto dataType : resultTypes)
        {
            visitRuntimeType(dataType.type, []<typename>() { });
            call.binding.resultTypes.push_back(dataType);
        }
        if (call.binding.resultTypes.empty())
        {
            throw CannotOpenSource("async function {} must declare at least one result", function);
        }
        for (const auto& [index, field] : outputs)
        {
            if (index >= call.binding.resultTypes.size())
            {
                throw CannotOpenSource("async function {} has no output {}", function, index);
            }
            requireSameType(outputFields.at(field).second, call.binding.resultTypes[index], "output field");
            outputValues.push_back({.call = calls.size(), .index = index, .field = QualifiedIdentifier::create(Identifier::parse(field))});
        }
        for (const auto& [dataType, field, constant, earlierCall, output] : arguments)
        {
            visitRuntimeType(dataType.type, []<typename>() { });
            call.binding.argumentTypes.push_back(dataType);
            if (static_cast<int>(field.has_value()) + static_cast<int>(constant.has_value()) + static_cast<int>(earlierCall.has_value())
                != 1)
            {
                throw CannotOpenSource("async argument must have exactly one field, constant or earlier-call binding");
            }
            if (field)
            {
                requireSameType(outputFields.at(*field).second, dataType, "argument field");
                const auto name = QualifiedIdentifier::create(Identifier::parse(*field));
                if (std::ranges::find(call.inputFields, name) == call.inputFields.end())
                {
                    call.inputFields.push_back(name);
                }
                call.arguments.push_back({.field = name, .call = std::nullopt, .output = 0, .constant = std::nullopt});
                call.binding.constantArguments.emplace_back(std::nullopt);
            }
            else if (earlierCall)
            {
                if (*earlierCall >= calls.size() || output >= calls[*earlierCall].binding.resultTypes.size())
                {
                    throw CannotOpenSource("async argument reads an invalid earlier call {} output {}", *earlierCall, output);
                }
                requireSameType(calls[*earlierCall].binding.resultTypes[output], dataType, "earlier call result");
                call.level = std::max(call.level, calls[*earlierCall].level + 1);
                call.arguments.push_back({.field = std::nullopt, .call = earlierCall, .output = output, .constant = std::nullopt});
                call.binding.constantArguments.emplace_back(std::nullopt);
            }
            else
            {
                const auto value = RuntimeValue::fromLiteral(dataType, *constant);
                call.arguments.push_back({.field = std::nullopt, .call = std::nullopt, .output = 0, .constant = value});
                call.binding.constantArguments.emplace_back(value);
            }
        }
        const auto factory = AsyncFunctionRegistry::instance().find(function);
        if (!factory)
        {
            throw UnknownFunctionType("no async implementation registered for {}", function);
        }
        /// The first instance tells what the function allows; every instance sees the same typed constant children.
        call.instances.push_back((*factory)(AsyncFunctionRegistryArguments{.binding = call.binding}));
        maxConcurrency = std::min(maxConcurrency, std::max<size_t>(1, call.instances.front()->maxConcurrency().value_or(maxConcurrency)));
        levels = std::max(levels, call.level + 1);
        calls.push_back(std::move(call));
    }
    for (auto& call : calls)
    {
        const auto factory = AsyncFunctionRegistry::instance().find(call.function);
        while (call.instances.size() < maxConcurrency)
        {
            call.instances.push_back((*factory)(AsyncFunctionRegistryArguments{.binding = call.binding}));
        }
    }
    cancelSignals = std::vector<boost::asio::cancellation_signal>(maxConcurrency);
    for (size_t slot = maxConcurrency; slot > 0; --slot)
    {
        freeSlots.push_back(slot - 1);
    }

    for (const auto& [name, inputOffset] : inputOffsets)
    {
        if (std::ranges::none_of(
                descriptions,
                [&](const AsyncCall& call)
                { return std::ranges::any_of(call.outputs, [&](const AsyncCallOutput& output) { return output.field == name; }); }))
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
                const std::scoped_lock lock(window.mutex);
                window.channelChanged = true;
            }
            window.changed.notify_all();
        });
}

void AsyncSource::close()
{
    /// Also after a failure or a stop request, which already drained: rows of this source must not outlive it.
    cancelAndDrain();
    channel.release();
}

AsyncValues AsyncSource::readArguments(
    const BoundCall& call, const TupleBuffer& input, const size_t row, const std::vector<AsyncCallResult>& results) const
{
    std::optional<Record> record;
    if (!call.inputFields.empty())
    {
        const RecordBuffer buffer{BorrowedNautilusBuffer::from(nautilus::val<const TupleBuffer*>{std::addressof(input)})};
        auto index = nautilus::val<uint64_t>{row};
        record = inputBufferRef->readRecord(call.inputFields, buffer, index);
    }
    std::vector<RuntimeValue> arguments;
    arguments.reserve(call.arguments.size());
    for (size_t index = 0; index < call.arguments.size(); ++index)
    {
        const auto& [field, earlierCall, output, constant] = call.arguments[index];
        if (earlierCall)
        {
            arguments.push_back(results[*earlierCall][output].toOwned());
        }
        else if (constant)
        {
            arguments.push_back(*constant);
        }
        else
        {
            arguments.push_back(fromVarVal(record->read(*field), call.binding.argumentTypes[index]));
        }
    }
    return AsyncValues{std::move(arguments)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<AsyncCallResult> AsyncSource::callWithRetries(const size_t callIndex, const size_t slot, AsyncValues arguments) const
{
    const auto& call = calls[callIndex];
    auto& function = *call.instances[slot];
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
            /// A bug in the function rather than a failure of the service it calls, so it is not retried.
            if (result->size() != call.binding.resultTypes.size())
            {
                throw CannotReadFromSource(
                    "async function {} produced {} values, but declares {}",
                    call.function,
                    result->size(),
                    call.binding.resultTypes.size());
            }
            for (size_t index = 0; index < result->size(); ++index)
            {
                const auto value = (*result)[index];
                const auto expected = call.binding.resultTypes[index];
                if (value.getDataType().type != expected.type || (value.isNull() && !expected.nullable))
                {
                    throw CannotReadFromSource(
                        "async function {} output {} has type {} (null: {}), expected {}",
                        call.function,
                        index,
                        value.getDataType(),
                        value.isNull(),
                        expected);
                }
            }
            co_return std::move(*result);
        }
        if (attempt >= maxRetries
            || (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
        {
            std::rethrow_exception(failure);
        }
        const auto backoff = RETRY_BACKOFF * (size_t{1} << std::min<size_t>(attempt, MAX_BACKOFF_DOUBLINGS));
        NES_WARNING(
            "Async call of {} failed, attempt {} of {} follows in {} ms", call.function, attempt + 2, maxRetries + 1, backoff.count());
        boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, backoff};
        co_await timer.async_wait(boost::asio::use_awaitable);
    }
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<void> AsyncSource::processRow(const size_t slot, const std::shared_ptr<InFlightBuffer> buffer, const size_t row)
{
    using CallOperation
        = decltype(boost::asio::co_spawn(strand, std::declval<boost::asio::awaitable<AsyncCallResult>>(), boost::asio::deferred));
    /// One result per call. Level by level: the calls of one level run in parallel and only read results of lower levels.
    std::vector<AsyncCallResult> results(calls.size());
    for (size_t level = 0; level < levels; ++level)
    {
        std::vector<size_t> started;
        std::vector<CallOperation> operations;
        for (size_t index = 0; index < calls.size(); ++index)
        {
            if (calls[index].level == level)
            {
                started.push_back(index);
                operations.push_back(
                    boost::asio::co_spawn(
                        strand,
                        callWithRetries(index, slot, readArguments(calls[index], buffer->input, row, results)),
                        boost::asio::deferred));
            }
        }
        auto [order, failures, values] = co_await boost::asio::experimental::make_parallel_group(std::move(operations))
                                             .async_wait(boost::asio::experimental::wait_for_all(), boost::asio::use_awaitable);
        for (size_t position = 0; position < started.size(); ++position)
        {
            if (failures[position])
            {
                std::rethrow_exception(failures[position]);
            }
            results[started[position]] = std::move(values[position]);
        }
    }
    std::vector<RuntimeValue> outputs;
    outputs.reserve(outputValues.size());
    for (const auto& output : outputValues)
    {
        outputs.push_back(results[output.call][output.index].toOwned());
    }
    buffer->results[row] = AsyncValues{std::move(outputs)};
}

void AsyncSource::startRows()
{
    bool started = false;
    while (!stopping && !freeSlots.empty())
    {
        std::shared_ptr<InFlightBuffer> buffer;
        size_t row = 0;
        {
            const std::scoped_lock lock(window.mutex);
            const auto next
                = std::ranges::find_if(window.buffers, [](const auto& candidate) { return candidate->nextRow < candidate->rows; });
            if (window.failure || next == window.buffers.end())
            {
                break;
            }
            buffer = *next;
            row = buffer->nextRow++;
            --window.unstartedRows;
            ++window.runningRows;
        }
        started = true;
        const auto slot = freeSlots.back();
        freeSlots.pop_back();
        boost::asio::co_spawn(
            strand,
            processRow(slot, buffer, row),
            boost::asio::bind_cancellation_slot(
                cancelSignals[slot].slot(), [this, buffer, slot](const std::exception_ptr& error) { finishRow(buffer, slot, error); }));
    }
    if (started)
    {
        /// Fewer rows wait to start, which may let the source thread take the next buffer.
        window.changed.notify_all();
    }
}

void AsyncSource::finishRow(const std::shared_ptr<InFlightBuffer>& buffer, const size_t slot, const std::exception_ptr& error)
{
    freeSlots.push_back(slot);
    {
        const std::scoped_lock lock(window.mutex);
        --window.runningRows;
        /// A row cancelled because the query stops did not fail.
        if (error && !stopping && !window.failure)
        {
            window.failure = error;
        }
        if (--buffer->unfinishedRows == 0)
        {
            buffer->done = true;
        }
    }
    window.changed.notify_all();
    startRows();
}

void AsyncSource::admit(TupleBuffer input)
{
    const auto rows = input.getNumberOfTuples();
    auto buffer = std::make_shared<InFlightBuffer>(InFlightBuffer{
        .input = std::move(input),
        .rows = rows,
        .nextRow = 0,
        .unfinishedRows = rows,
        .results = std::vector<AsyncValues>(rows),
        .done = rows == 0});
    {
        const std::scoped_lock lock(window.mutex);
        window.buffers.push_back(std::move(buffer));
        window.unstartedRows += rows;
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
        Record results;
        for (size_t position = 0; position < outputValues.size(); ++position)
        {
            results.write(outputValues[position].field, toVarVal(buffer.results[row][position]));
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
            for (auto& signal : cancelSignals)
            {
                signal.emit(boost::asio::cancellation_type::terminal);
            }
        });
    {
        std::unique_lock lock(window.mutex);
        window.changed.wait(lock, [this] { return window.runningRows == 0; });
        window.buffers.clear();
        window.unstartedRows = 0;
    }
    /// The strand runs handlers in the order they were posted: once this one ran, no handler of this source is left.
    std::promise<void> drained;
    boost::asio::post(strand, [&drained] { drained.set_value(); });
    drained.get_future().wait();
}

Source::FillTupleBufferResult AsyncSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    std::unique_lock lock(window.mutex);
    while (true)
    {
        if (window.failure)
        {
            const auto failure = window.failure;
            lock.unlock();
            cancelAndDrain();
            std::rethrow_exception(failure);
        }
        if (!window.buffers.empty() && window.buffers.front()->done)
        {
            const auto buffer = std::move(window.buffers.front());
            window.buffers.pop_front();
            lock.unlock();
            writeOutput(*buffer, tupleBuffer);
            return FillTupleBufferResult::withBytes(buffer->rows);
        }
        if (inputExhausted && window.buffers.empty())
        {
            return FillTupleBufferResult::eos();
        }

        /// The next buffer is taken once the rows waiting to start would no longer keep every slot busy, so the calls
        /// run across buffer boundaries while the channel, not this source, holds what the calls cannot keep up with.
        const bool wantsInput = !inputExhausted && window.buffers.size() < maxConcurrency && window.unstartedRows < maxConcurrency;
        if (wantsInput)
        {
            const bool idle = window.buffers.empty();
            window.channelChanged = false;
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
        { return window.failure || (!window.buffers.empty() && window.buffers.front()->done) || (wantsInput && window.channelChanged); };
        if (!window.changed.wait(lock, stopToken, progressed))
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
