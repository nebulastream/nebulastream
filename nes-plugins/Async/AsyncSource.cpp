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
#include <cstring>
#include <exception>
#include <expected>
#include <future>
#include <memory>
#include <mutex>
#include <ostream>
#include <ranges>
#include <span>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/AsyncResourceLimiter.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <Util/Variant.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <fmt/format.h>
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

const ConfigField<size_t> TRANSIENT_RETRIES{
    Identifier::parse("TRANSIENT_RETRIES"),
    "Further attempts of a call after transient failures, such as a service answering 429, with backoff and jitter",
    sizeFromLiteral,
    AsyncSourceConfig{}.transientRetries};

const ConfigField<size_t> READ_AHEAD_ROWS{
    Identifier::parse("READ_AHEAD_ROWS"),
    "Rows the source holds at most before it stops taking input; 0 for 64 per row the slots take at once",
    sizeFromLiteral,
    size_t{0}};

const ConfigField<size_t> TRANSIENT_RETRY_SECONDS{
    Identifier::parse("TRANSIENT_RETRY_SECONDS"),
    "Seconds from a call's first attempt within which retries after transient failures have to be made",
    sizeFromLiteral,
    AsyncSourceConfig{}.transientRetrySeconds};

/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

/// One event loop for all Async sources, so concurrent calls cost neither a thread per call nor a thread per source.
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

/// Arguments and results are non-nullable VARSIZED only, which is what HTTP_CALL needs
void requireVarSized(const DataType& type, std::string_view what)
{
    if (!type.isType(DataType::Type::VARSIZED) || type.nullable)
    {
        throw NotImplemented("async {} of type {}, only non-nullable VARSIZED is supported", what, type);
    }
}
}

Schema<QualifiedErasedConfigField, Ordered> AsyncSource::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("ASYNC_SOURCE"),
        CHANNEL,
        CALLS,
        INPUT_FIELDS,
        MAX_CONCURRENCY,
        CHANNEL_CAPACITY,
        MAX_RETRIES,
        TRANSIENT_RETRIES,
        TRANSIENT_RETRY_SECONDS,
        READ_AHEAD_ROWS);
}

std::expected<AsyncSourceConfig, Exception> AsyncSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return AsyncSourceConfig{
        .channel = config.get(CHANNEL),
        .calls = config.get(CALLS),
        .inputFields = config.get(INPUT_FIELDS),
        .maxConcurrency = config.get(MAX_CONCURRENCY),
        .channelCapacity = config.get(CHANNEL_CAPACITY),
        .maxRetries = config.get(MAX_RETRIES),
        .transientRetries = config.get(TRANSIENT_RETRIES),
        .transientRetrySeconds = config.get(TRANSIENT_RETRY_SECONDS),
        .readAheadRows = config.get(READ_AHEAD_ROWS)};
}

AsyncSource::AsyncSource(const AsyncSourceConfig& config, const SourceDescriptor& sourceDescriptor)
    : channelId(config.channel)
    , maxConcurrency(std::max<size_t>(1, config.maxConcurrency))
    , retryPolicy{
          .maxRetries = config.maxRetries,
          .maxTransientRetries = config.transientRetries,
          .transientTimeLimit = std::chrono::seconds{config.transientRetrySeconds}}
    , channel(config.channel, config.channelCapacity, HandoffChannel::Role::SOURCE)
    , strand(boost::asio::make_strand(SharedEventLoop::context()))
{
    const auto descriptions = rfl::json::read<std::vector<AsyncCall>>(config.calls);
    if (!descriptions)
    {
        throw CannotOpenSource("malformed CALLS of async source: {}", descriptions.error().what());
    }

    /// Row layout: fields packed in order, each taking its size including the null flag.
    std::unordered_map<std::string, std::pair<size_t, DataType>> outputFields;
    for (const auto& field : sourceDescriptor.getSchema())
    {
        outputFields.emplace(fmt::format("{}", field.getFullyQualifiedName()), std::pair{outputRowSize, field.getDataType()});
        outputRowSize += field.getDataType().getSizeInBytesWithNull();
    }
    std::unordered_map<std::string, size_t> inputOffsets;
    for (const auto& name : config.inputFields | std::views::split(',') | std::ranges::to<std::vector<std::string>>())
    {
        const auto field = outputFields.find(name);
        if (field == outputFields.end())
        {
            throw CannotOpenSource("handed-off field {} is not produced by the async source", name);
        }
        inputOffsets.emplace(name, inputRowSize);
        inputRowSize += field->second.second.getSizeInBytesWithNull();
    }
    INVARIANT(inputRowSize == outputRowSize, "handed-off rows must have the async source's fields");

    for (const auto& [outputs, function, configuration, arguments] : descriptions.value())
    {
        BoundCall call{
            .function = function,
            .configuration = configuration,
            .arguments = {},
            .requiredValues = 1,
            .level = 0,
            .orderingKey = std::nullopt,
            .maxBatchSize = 1,
            .instances = {},
            .limiter = nullptr};
        for (const auto& [index, field] : outputs)
        {
            const auto& [outputOffset, outputType] = outputFields.at(field);
            requireVarSized(outputType, "result");
            outputValues.push_back({.call = calls.size(), .index = index, .outputOffset = outputOffset});
            call.requiredValues = std::max(call.requiredValues, index + 1);
        }
        for (const auto& [field, constant, earlierCall, output] : arguments)
        {
            if (field)
            {
                requireVarSized(outputFields.at(*field).second, "argument");
                call.arguments.push_back({.inputOffset = inputOffsets.at(*field), .call = std::nullopt, .output = 0, .constant = {}});
            }
            else if (earlierCall)
            {
                if (*earlierCall >= calls.size())
                {
                    throw CannotOpenSource("async call reads call {}, which is not listed before it", *earlierCall);
                }
                call.level = std::max(call.level, calls[*earlierCall].level + 1);
                calls[*earlierCall].requiredValues = std::max(calls[*earlierCall].requiredValues, output + 1);
                call.arguments.push_back({.inputOffset = std::nullopt, .call = earlierCall, .output = output, .constant = {}});
            }
            else
            {
                call.arguments.push_back(
                    {.inputOffset = std::nullopt, .call = std::nullopt, .output = 0, .constant = constant.value_or("")});
            }
        }
        const auto factory = AsyncFunctionRegistry::instance().find(function);
        if (!factory)
        {
            throw UnknownFunctionType("no async implementation registered for {}", function);
        }
        /// The first instance tells what the function allows; the stage runs within the strictest bounds of its functions.
        call.instances.push_back((*factory)(AsyncFunctionRegistryArguments{.configuration = configuration}));
        const auto& first = *call.instances.front();
        maxConcurrency = std::min(maxConcurrency, std::max<size_t>(1, first.maxConcurrency().value_or(maxConcurrency)));
        call.maxBatchSize = std::max<size_t>(1, first.maxBatchSize());
        batchSize = std::max(batchSize, call.maxBatchSize);
        if (const auto shared = first.sharedLimit())
        {
            call.limiter = AsyncResourceLimiter::of(shared->resource, shared->limit);
        }
        if (const auto key = first.orderingKey())
        {
            if (*key >= call.arguments.size() || call.arguments[*key].call.has_value())
            {
                throw CannotOpenSource(
                    "async function {} orders its calls by argument {}, which is not a handed-off field or a constant", function, *key);
            }
            call.orderingKey = key;
            keyed = true;
        }
        levels = std::max(levels, call.level + 1);
        calls.push_back(std::move(call));
    }
    for (auto& call : calls)
    {
        const auto factory = AsyncFunctionRegistry::instance().find(call.function);
        while (call.instances.size() < maxConcurrency)
        {
            call.instances.push_back((*factory)(AsyncFunctionRegistryArguments{.configuration = call.configuration}));
        }
    }
    readAheadRows = std::max(
        maxConcurrency * batchSize,
        config.readAheadRows != 0 ? config.readAheadRows : maxConcurrency * batchSize * AsyncSourceConfig::DEFAULT_READ_AHEAD_FACTOR);
    cancelSignals = std::vector<boost::asio::cancellation_signal>(maxConcurrency);
    for (size_t slot = maxConcurrency; slot > 0; --slot)
    {
        freeSlots.push_back(slot - 1);
    }

    for (const auto& [name, inputOffset] : inputOffsets)
    {
        if (std::ranges::none_of(
                descriptions.value(),
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
    NES_DEBUG("Async source {}: at most {} buffers waited for an earlier one of their origin", channelId, mostReordered);
    NES_DEBUG(
        "Async source {}: {} buffers of {} rows on average, at most {} held at once; the read-ahead bound of {} rows held back input "
        "for {} ms while fewer rows were ready than the slots take",
        channelId,
        admittedBuffers,
        admittedBuffers == 0 ? 0.0 : static_cast<double>(admittedRows) / static_cast<double>(admittedBuffers),
        mostBuffersHeld,
        readAheadRows,
        std::chrono::duration_cast<std::chrono::milliseconds>(boundedWhileShort).count());
    /// Also after a failure or a stop request, which already drained: rows of this source must not outlive it.
    cancelAndDrain();
    if (transientRetriesMade + otherRetriesMade > 0)
    {
        NES_INFO(
            "Async source {}: {} retries after transient failures, {} after other failures, {} ms backed off",
            channelId,
            transientRetriesMade,
            otherRetriesMade,
            backedOff.count());
    }
    channel.release();
}

std::vector<std::string> AsyncSource::readArguments(
    const BoundCall& call, const TupleBuffer& input, const size_t row, const std::vector<AsyncCallResult>& results) const
{
    const auto rowBytes = input.getAvailableMemoryArea().subspan(row * inputRowSize, inputRowSize);
    std::vector<std::string> arguments;
    for (const auto& [inputOffset, earlierCall, output, constant] : call.arguments)
    {
        if (earlierCall)
        {
            arguments.push_back(results[*earlierCall][output]);
            continue;
        }
        if (!inputOffset)
        {
            arguments.push_back(constant);
            continue;
        }
        VariableSizedAccess access;
        std::memcpy(&access, rowBytes.subspan(*inputOffset).data(), sizeof(access));
        const auto value = TupleBufferRef::loadAssociatedVarSizedValue(input, access);
        arguments.emplace_back(reinterpret_cast<const char*>(value.data()), value.size());
    }
    return arguments;
}

std::vector<std::string> AsyncSource::readKeys(const TupleBuffer& input, const size_t row) const
{
    std::vector<std::string> keys;
    const auto rowBytes = input.getAvailableMemoryArea().subspan(row * inputRowSize, inputRowSize);
    for (size_t index = 0; index < calls.size(); ++index)
    {
        if (!calls[index].orderingKey)
        {
            continue;
        }
        const auto& argument = calls[index].arguments[*calls[index].orderingKey];
        std::string value = argument.constant;
        if (argument.inputOffset)
        {
            VariableSizedAccess access;
            std::memcpy(&access, rowBytes.subspan(*argument.inputOffset).data(), sizeof(access));
            const auto bytes = TupleBufferRef::loadAssociatedVarSizedValue(input, access);
            value.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        }
        /// Keys of different calls never collide: each call orders only its own rows.
        keys.push_back(fmt::format("{}:{}", index, value));
    }
    return keys;
}

namespace
{
boost::asio::awaitable<bool> cancelled()
{
    co_return (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none;
}

/// A failed attempt: what the query fails with if it is not retried, and whether the function marked it transient.
struct Failure
{
    std::exception_ptr error;
    bool transient = false;
    std::optional<std::chrono::milliseconds> retryAfter;
};

Failure classify(const std::exception_ptr& error)
{
    try
    {
        std::rethrow_exception(error);
    }
    catch (const AsyncTransientFailure& transient)
    {
        return {.error = std::make_exception_ptr(transient.failure()), .transient = true, .retryAfter = transient.retryAfter()};
    }
    /// Only classifies; the failure itself is kept unchanged.
    catch (...) /// NOLINT(no-raw-catch-all)
    {
        return {.error = error, .transient = false, .retryAfter = std::nullopt};
    }
}
}

boost::asio::awaitable<void> AsyncSource::backOff(
    const BoundCall& call, AsyncFunction& function, const std::chrono::milliseconds delay, const bool transient, const size_t rows)
{
    NES_WARNING(
        "Async call of {} for {} row(s) failed{}; trying again in {} ms", call.function, rows, transient ? " transiently" : "", delay.count());
    function.retryScheduled(delay, transient);
    ++(transient ? transientRetriesMade : otherRetriesMade);
    backedOff += delay;
    boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, delay};
    co_await timer.async_wait(boost::asio::use_awaitable);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<AsyncCallResult> AsyncSource::callWithRetries(const size_t callIndex, const size_t slot, std::vector<std::string> arguments)
{
    const auto& call = calls[callIndex];
    auto& function = *call.instances[slot];
    RetryBudget budget{retryPolicy, std::chrono::steady_clock::now()};
    while (true)
    {
        std::optional<AsyncCallResult> result;
        std::exception_ptr error;
        try
        {
            /// Held only while the request is out, not during the backoff.
            const auto permit = call.limiter ? co_await call.limiter->acquire() : AsyncResourceLimiter::Permit{};
            result = co_await function.callValues(arguments);
        }
        /// The failure is kept and rethrown unchanged; CPPTRACE_TRY cannot wrap a co_await.
        catch (...) /// NOLINT(no-raw-catch-all)
        {
            error = std::current_exception();
        }
        if (result)
        {
            /// A bug in the function rather than a failure of the service it calls, so it is not retried.
            if (result->size() < call.requiredValues)
            {
                throw CannotReadFromSource(
                    "async function {} produced {} values, but the query reads {}", call.function, result->size(), call.requiredValues);
            }
            co_return std::move(*result);
        }
        const auto failure = classify(error);
        if (co_await cancelled())
        {
            std::rethrow_exception(failure.error);
        }
        const auto delay = budget.next(
            failure.transient, failure.retryAfter, std::chrono::steady_clock::now(), std::uniform_real_distribution<double>{0.0, 1.0}(jitter));
        if (!delay)
        {
            std::rethrow_exception(failure.error);
        }
        co_await backOff(call, function, *delay, failure.transient, 1);
    }
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<std::vector<AsyncCallResult>>
AsyncSource::callBatchWithRetries(const size_t callIndex, const size_t slot, std::vector<std::vector<std::string>> rows)
{
    const auto& call = calls[callIndex];
    auto& function = *call.instances[slot];
    std::optional<AsyncBatchResult> batch;
    RetryBudget budget{retryPolicy, std::chrono::steady_clock::now()};
    while (true)
    {
        std::exception_ptr error;
        try
        {
            const auto permit = call.limiter ? co_await call.limiter->acquire() : AsyncResourceLimiter::Permit{};
            batch = co_await function.callBatch(rows);
        }
        /// The failure is kept and rethrown unchanged; CPPTRACE_TRY cannot wrap a co_await.
        catch (...) /// NOLINT(no-raw-catch-all)
        {
            error = std::current_exception();
        }
        if (batch)
        {
            break;
        }
        const auto failure = classify(error);
        if (co_await cancelled())
        {
            std::rethrow_exception(failure.error);
        }
        const auto delay = budget.next(
            failure.transient, failure.retryAfter, std::chrono::steady_clock::now(), std::uniform_real_distribution<double>{0.0, 1.0}(jitter));
        if (!delay)
        {
            /// A service that stays overloaded would only get more requests from calling the rows one by one.
            if (failure.transient)
            {
                std::rethrow_exception(failure.error);
            }
            NES_WARNING("Async batch of {} rows of {} failed; calling its rows one by one", rows.size(), call.function);
            break;
        }
        co_await backOff(call, function, *delay, failure.transient, rows.size());
    }
    if (batch && batch->size() != rows.size())
    {
        throw CannotReadFromSource("async function {} answered a batch of {} rows with {} results", call.function, rows.size(), batch->size());
    }
    std::vector<AsyncCallResult> results;
    results.reserve(rows.size());
    for (size_t row = 0; row < rows.size(); ++row)
    {
        if (batch && (*batch)[row])
        {
            if ((*batch)[row]->size() < call.requiredValues)
            {
                throw CannotReadFromSource(
                    "async function {} produced {} values, but the query reads {}", call.function, (*batch)[row]->size(), call.requiredValues);
            }
            results.push_back(std::move(*(*batch)[row]));
            continue;
        }
        results.push_back(co_await callWithRetries(callIndex, slot, std::move(rows[row])));
    }
    co_return results;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<std::vector<AsyncCallResult>>
AsyncSource::callRows(const size_t callIndex, const size_t slot, std::vector<std::vector<std::string>> rows)
{
    const auto requestSize = calls[callIndex].maxBatchSize;
    std::vector<AsyncCallResult> results;
    results.reserve(rows.size());
    if (requestSize == 1 || rows.size() == 1)
    {
        /// One request per row; with a slot's single instance, one after the other.
        for (auto& arguments : rows)
        {
            results.push_back(co_await callWithRetries(callIndex, slot, std::move(arguments)));
        }
        co_return results;
    }
    for (size_t begin = 0; begin < rows.size(); begin += requestSize)
    {
        const auto end = std::min(rows.size(), begin + requestSize);
        std::vector<std::vector<std::string>> request(
            std::make_move_iterator(rows.begin() + static_cast<std::ptrdiff_t>(begin)),
            std::make_move_iterator(rows.begin() + static_cast<std::ptrdiff_t>(end)));
        for (auto& values : co_await callBatchWithRetries(callIndex, slot, std::move(request)))
        {
            results.push_back(std::move(values));
        }
    }
    co_return results;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<void> AsyncSource::processBatch(const size_t slot, const std::vector<RowRef> rows)
{
    using CallOperation = decltype(boost::asio::co_spawn(
        strand, std::declval<boost::asio::awaitable<std::vector<AsyncCallResult>>>(), boost::asio::deferred));
    /// Per row one result per call. Level by level: the calls of one level run in parallel and only read results of lower
    /// levels; each call takes all rows of the batch.
    std::vector<std::vector<AsyncCallResult>> results(rows.size(), std::vector<AsyncCallResult>(calls.size()));
    for (size_t level = 0; level < levels; ++level)
    {
        std::vector<size_t> started;
        std::vector<CallOperation> operations;
        for (size_t index = 0; index < calls.size(); ++index)
        {
            if (calls[index].level == level)
            {
                std::vector<std::vector<std::string>> arguments;
                arguments.reserve(rows.size());
                for (size_t position = 0; position < rows.size(); ++position)
                {
                    arguments.push_back(readArguments(calls[index], rows[position].buffer->input, rows[position].row, results[position]));
                }
                started.push_back(index);
                operations.push_back(boost::asio::co_spawn(strand, callRows(index, slot, std::move(arguments)), boost::asio::deferred));
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
            for (size_t row = 0; row < rows.size(); ++row)
            {
                results[row][started[position]] = std::move(values[position][row]);
            }
        }
    }
    for (size_t position = 0; position < rows.size(); ++position)
    {
        auto& outputs = rows[position].buffer->results[rows[position].row];
        outputs.reserve(outputValues.size());
        for (const auto& [call, index, outputOffset] : outputValues)
        {
            outputs.push_back(results[position][call][index]);
        }
    }
}

void AsyncSource::startRows()
{
    bool started = false;
    while (!stopping && !freeSlots.empty())
    {
        std::vector<std::pair<RowRef, std::vector<std::string>>> taken;
        {
            const std::scoped_lock lock(window.mutex);
            if (window.failure)
            {
                break;
            }
            /// Whatever is ready, up to a full batch: rows only pile up while every slot is busy.
            taken = window.queue.take(batchSize);
            if (taken.empty())
            {
                break;
            }
            window.runningRows += taken.size();
            window.startableRows = keyed ? window.queue.startableRows() : window.queue.size();
        }
        started = true;
        std::vector<RowRef> rows;
        std::vector<std::string> keys;
        for (auto& [row, rowKeys] : taken)
        {
            rows.push_back(std::move(row));
            keys.insert(keys.end(), std::make_move_iterator(rowKeys.begin()), std::make_move_iterator(rowKeys.end()));
        }
        const auto slot = freeSlots.back();
        freeSlots.pop_back();
        boost::asio::co_spawn(
            strand,
            processBatch(slot, rows),
            boost::asio::bind_cancellation_slot(
                cancelSignals[slot].slot(),
                [this, rows, keys = std::move(keys), slot](const std::exception_ptr& error) { finishBatch(rows, keys, slot, error); }));
    }
    if (started)
    {
        /// Fewer rows wait to start, which may let the source thread take the next buffer.
        window.changed.notify_all();
    }
}

void AsyncSource::finishBatch(
    const std::vector<RowRef>& rows, const std::vector<std::string>& keys, const size_t slot, const std::exception_ptr& error)
{
    freeSlots.push_back(slot);
    {
        const std::scoped_lock lock(window.mutex);
        window.runningRows -= rows.size();
        /// A row cancelled because the query stops did not fail.
        if (error && !stopping && !window.failure)
        {
            window.failure = error;
        }
        window.queue.finish(keys);
        if (keyed)
        {
            window.startableRows = window.queue.startableRows();
        }
        for (const auto& [buffer, row] : rows)
        {
            if (--buffer->unfinishedRows == 0)
            {
                buffer->done = true;
            }
        }
    }
    window.changed.notify_all();
    startRows();
}

void AsyncSource::admit(TupleBuffer input)
{
    const auto rows = input.getNumberOfTuples();
    std::vector<std::vector<std::string>> keys(rows);
    if (keyed)
    {
        for (size_t row = 0; row < rows; ++row)
        {
            keys[row] = readKeys(input, row);
        }
    }
    ++admittedBuffers;
    admittedRows += rows;
    auto buffer = std::make_shared<InFlightBuffer>(InFlightBuffer{
        .input = std::move(input),
        .rows = rows,
        .unfinishedRows = rows,
        .results = std::vector<std::vector<std::string>>(rows),
        .done = rows == 0});
    {
        const std::scoped_lock lock(window.mutex);
        for (size_t row = 0; row < rows; ++row)
        {
            window.queue.push(RowRef{.buffer = buffer, .row = row}, std::move(keys[row]));
        }
        window.buffers.push_back(std::move(buffer));
        window.heldRows += rows;
        mostBuffersHeld = std::max(mostBuffersHeld, window.buffers.size());
        window.startableRows = keyed ? window.queue.startableRows() : window.queue.size();
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
        for (size_t position = 0; position < outputValues.size(); ++position)
        {
            const auto access
                = TupleBufferRef::writeVarSized(output, *bufferProvider, std::as_bytes(std::span{buffer.results[row][position]}));
            std::memcpy(out.subspan((row * outputRowSize) + outputValues[position].outputOffset).data(), &access, sizeof(access));
        }
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
        window.heldRows = 0;
        window.queue.clear();
        window.startableRows = 0;
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
            window.heldRows -= buffer->rows;
            lock.unlock();
            writeOutput(*buffer, tupleBuffer);
            return FillTupleBufferResult::withBytes(buffer->rows);
        }
        if (inputExhausted && window.buffers.empty())
        {
            return FillTupleBufferResult::eos();
        }

        /// The next buffer is taken once the rows ready to start would no longer fill every slot with a full batch, so the
        /// calls run across buffer boundaries while the channel, not this source, holds what the calls cannot keep up with.
        /// Rows that wait for their key do not count as ready, so a busy key does not keep other keys from starting; the
        /// source reads ahead past them up to `readAheadRows` held rows. The bound counts rows, not buffers: over input
        /// sorted by key, every key in flight needs all its rows before the next key's in the window.
        const auto rowsInFlight = maxConcurrency * batchSize;
        const bool wantsInput = !inputExhausted && window.heldRows < readAheadRows && window.startableRows < rowsInFlight;
        const bool boundedWhileShortNow = !inputExhausted && window.heldRows >= readAheadRows && window.startableRows < rowsInFlight;
        if (boundedWhileShortNow && !boundedSince)
        {
            boundedSince = std::chrono::steady_clock::now();
        }
        else if (!boundedWhileShortNow && boundedSince)
        {
            boundedWhileShort += std::chrono::steady_clock::now() - *boundedSince;
            boundedSince.reset();
        }
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
                const StreamPosition position{
                    .origin = input->getOriginId().getRawValue(),
                    .sequence = input->getSequenceNumber().getRawValue(),
                    .chunk = input->getChunkNumber().getRawValue(),
                    .lastChunk = input->isLastChunk()};
                for (auto& next : reorderer.push(position, std::move(*input)))
                {
                    admit(std::move(next));
                }
                mostReordered = std::max(mostReordered, reorderer.size());
            }
            else
            {
                inputExhausted = channel->exhausted();
                if (inputExhausted && reorderer.size() > 0)
                {
                    /// Only a producer that skips sequence numbers gets here; what it sent still goes out, in order.
                    NES_WARNING("Async source {}: {} buffers never got their predecessor; emitting them in order", channelId, reorderer.size());
                    for (auto& next : reorderer.drain())
                    {
                        admit(std::move(next));
                    }
                }
            }
            lock.lock();
            if (input || inputExhausted)
            {
                continue;
            }
        }
        /// Wakes for the next buffer once the slots took enough rows that the input is wanted again (`startRows`
        /// notifies), not only once the front buffer is done: a buffer holds more rows than one round of calls, and
        /// waiting for all of them left the slots idle for about one call per buffer.
        const auto progressed = [&]
        {
            const bool wantsInputNow = !inputExhausted && window.heldRows < readAheadRows && window.startableRows < rowsInFlight;
            return window.failure || (!window.buffers.empty() && window.buffers.front()->done)
                || (wantsInputNow && (!wantsInput || window.channelChanged));
        };
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
