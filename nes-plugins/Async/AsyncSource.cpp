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
#include <future>
#include <memory>
#include <ostream>
#include <ranges>
#include <span>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/experimental/parallel_group.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <fmt/format.h>
#include <Util/Logger/Logger.hpp>
#include <rfl/json/read.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <HandoffChannel.hpp>

namespace NES
{

namespace
{
/// One event loop for all Async sources, so concurrent calls cost neither a thread per call nor a thread per source.
/// ponytail: one loop thread; add threads if completions get expensive, the per-source strands keep that safe
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

/// The calls of one buffer, shared with the completion handlers and with a stop request that may
/// arrive while they run.
struct PendingCalls
{
    size_t remaining = 0;
    std::exception_ptr failure;
    std::promise<void> done;
    std::vector<boost::asio::cancellation_signal> cancel;
};

constexpr std::chrono::milliseconds RETRY_BACKOFF{100};
/// Caps the backoff at 100 ms * 2^10, about 100 s.
constexpr size_t MAX_BACKOFF_DOUBLINGS = 10;

/// ponytail: arguments and results are non-nullable VARSIZED only, which is what HTTP_CALL needs
void requireVarSized(const DataType& type, std::string_view what)
{
    if (!type.isType(DataType::Type::VARSIZED) || type.nullable)
    {
        throw NotImplemented("async {} of type {}, only non-nullable VARSIZED is supported", what, type);
    }
}
}

AsyncSource::AsyncSource(const SourceDescriptor& sourceDescriptor)
    : channelId(sourceDescriptor.getFromConfig(ConfigParametersAsync::CHANNEL))
    , channelCapacity(sourceDescriptor.getFromConfig(ConfigParametersAsync::CHANNEL_CAPACITY))
    , maxConcurrency(std::max<size_t>(1, sourceDescriptor.getFromConfig(ConfigParametersAsync::MAX_CONCURRENCY)))
    , maxRetries(sourceDescriptor.getFromConfig(ConfigParametersAsync::MAX_RETRIES))
    , strand(boost::asio::make_strand(SharedEventLoop::context()))
{
    const auto descriptions = rfl::json::read<std::vector<AsyncCall>>(sourceDescriptor.getFromConfig(ConfigParametersAsync::CALLS));
    if (!descriptions)
    {
        throw CannotOpenSource("malformed CALLS of async source: {}", descriptions.error().what());
    }

    /// Row layout: fields packed in order, each taking its size including the null flag.
    std::unordered_map<std::string, std::pair<size_t, DataType>> outputFields;
    for (const auto& field : *sourceDescriptor.getLogicalSource().getSchema())
    {
        outputFields.emplace(fmt::format("{}", field.getFullyQualifiedName()), std::pair{outputRowSize, field.getDataType()});
        outputRowSize += field.getDataType().getSizeInBytesWithNull();
    }
    std::unordered_map<std::string, size_t> inputOffsets;
    for (const auto& name : sourceDescriptor.getFromConfig(ConfigParametersAsync::INPUT_FIELDS) | std::views::split(',')
             | std::ranges::to<std::vector<std::string>>())
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

    for (const auto& [output, function, arguments] : descriptions.value())
    {
        BoundCall call{.arguments = {}, .outputOffset = std::nullopt, .level = 0, .instances = {}};
        if (output)
        {
            const auto& [outputOffset, outputType] = outputFields.at(*output);
            requireVarSized(outputType, "result");
            call.outputOffset = outputOffset;
        }
        for (const auto& [field, constant, earlierCall] : arguments)
        {
            if (field)
            {
                requireVarSized(outputFields.at(*field).second, "argument");
                call.arguments.push_back({.inputOffset = inputOffsets.at(*field), .call = std::nullopt, .constant = {}});
            }
            else if (earlierCall)
            {
                if (*earlierCall >= calls.size())
                {
                    throw CannotOpenSource("async call reads call {}, which is not listed before it", *earlierCall);
                }
                call.level = std::max(call.level, calls[*earlierCall].level + 1);
                call.arguments.push_back({.inputOffset = std::nullopt, .call = *earlierCall, .constant = {}});
            }
            else
            {
                call.arguments.push_back({.inputOffset = std::nullopt, .call = std::nullopt, .constant = constant.value_or("")});
            }
        }
        const auto factory = AsyncFunctionRegistry::instance().find(function);
        if (!factory)
        {
            throw UnknownFunctionType("no async implementation registered for {}", function);
        }
        for (size_t worker = 0; worker < maxConcurrency; ++worker)
        {
            call.instances.push_back((*factory)(AsyncFunctionRegistryArguments{}));
        }
        levels = std::max(levels, call.level + 1);
        calls.push_back(std::move(call));
    }

    for (const auto& [name, inputOffset] : inputOffsets)
    {
        if (std::ranges::none_of(descriptions.value(), [&](const AsyncCall& call) { return call.output == name; }))
        {
            const auto& [outputOffset, type] = outputFields.at(name);
            copiedFields.push_back({.inputOffset = inputOffset, .outputOffset = outputOffset, .size = type.getSizeInBytesWithNull()});
        }
    }
}

void AsyncSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    channel = HandoffChannel::get(channelId, channelCapacity);
}

void AsyncSource::close()
{
    channel.reset();
    HandoffChannel::release(channelId);
}

std::vector<std::string> AsyncSource::readArguments(
    const BoundCall& call, const TupleBuffer& input, const size_t row, const std::vector<std::string>& results) const
{
    const auto rowBytes = input.getAvailableMemoryArea().subspan(row * inputRowSize, inputRowSize);
    std::vector<std::string> arguments;
    for (const auto& [inputOffset, earlierCall, constant] : call.arguments)
    {
        if (earlierCall)
        {
            arguments.push_back(results[*earlierCall]);
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

boost::asio::awaitable<std::string> AsyncSource::callWithRetries(AsyncFunction& function, std::vector<std::string> arguments) const
{
    for (size_t attempt = 0;; ++attempt)
    {
        std::exception_ptr failure;
        try
        {
            co_return co_await function.call(arguments);
        }
        catch (...)
        {
            failure = std::current_exception();
        }
        if (attempt >= maxRetries
            || (co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
        {
            std::rethrow_exception(failure);
        }
        const auto backoff = RETRY_BACKOFF * (size_t{1} << std::min<size_t>(attempt, MAX_BACKOFF_DOUBLINGS));
        NES_WARNING("Async call failed, attempt {} of {} follows in {} ms", attempt + 2, maxRetries + 1, backoff.count());
        boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, backoff};
        co_await timer.async_wait(boost::asio::use_awaitable);
    }
}

boost::asio::awaitable<void>
AsyncSource::processRows(const size_t worker, const TupleBuffer& input, TupleBuffer& output, const size_t rows, size_t& nextRow)
{
    using CallOperation
        = decltype(boost::asio::co_spawn(strand, std::declval<boost::asio::awaitable<std::string>>(), boost::asio::deferred));
    while (nextRow < rows)
    {
        const auto row = nextRow++;
        /// Level by level: the calls of one level run in parallel and only read results of lower levels.
        std::vector<std::string> results(calls.size());
        for (size_t level = 0; level < levels; ++level)
        {
            std::vector<size_t> started;
            std::vector<CallOperation> operations;
            for (size_t index = 0; index < calls.size(); ++index)
            {
                if (calls[index].level == level)
                {
                    started.push_back(index);
                    operations.push_back(boost::asio::co_spawn(
                        strand,
                        callWithRetries(*calls[index].instances[worker], readArguments(calls[index], input, row, results)),
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
        for (size_t index = 0; index < calls.size(); ++index)
        {
            if (const auto outputOffset = calls[index].outputOffset)
            {
                const auto access = TupleBufferRef::writeVarSized(output, *bufferProvider, std::as_bytes(std::span{results[index]}));
                std::memcpy(output.getAvailableMemoryArea().subspan((row * outputRowSize) + *outputOffset).data(), &access, sizeof(access));
            }
        }
    }
}

Source::FillTupleBufferResult AsyncSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    auto input = channel->pop(stopToken);
    if (!input)
    {
        return FillTupleBufferResult::eos();
    }

    tupleBuffer.setSequenceNumber(input->getSequenceNumber());
    tupleBuffer.setChunkNumber(input->getChunkNumber());
    tupleBuffer.setLastChunk(input->isLastChunk());
    tupleBuffer.setOriginId(input->getOriginId());
    tupleBuffer.setWatermark(input->getWatermark());
    tupleBuffer.setCreationTimestampInMS(input->getCreationTimestampInMS());
    /// Stored in the same order, so the child indices in the copied VARSIZED fields stay valid.
    for (uint32_t child = 0; child < input->getNumberOfChildBuffers(); ++child)
    {
        auto childBuffer = input->loadChildBuffer(ChildBufferIndex{child});
        [[maybe_unused]] const auto index = tupleBuffer.storeChildBuffer(childBuffer);
    }

    const auto rows = input->getNumberOfTuples();
    PRECONDITION(rows * outputRowSize <= tupleBuffer.getBufferSize(), "handed-off buffer holds more rows than the output buffer");
    const auto in = input->getAvailableMemoryArea();
    const auto out = tupleBuffer.getAvailableMemoryArea();
    for (size_t row = 0; row < rows; ++row)
    {
        for (const auto& [inputOffset, outputOffset, size] : copiedFields)
        {
            std::memcpy(out.subspan((row * outputRowSize) + outputOffset).data(), in.subspan((row * inputRowSize) + inputOffset).data(), size);
        }
    }

    /// ponytail: the next buffer waits until every call of this one finished; overlap buffers if that idles the service
    const auto workers = std::min<size_t>(maxConcurrency, rows);
    if (workers == 0)
    {
        return FillTupleBufferResult::withBytes(0);
    }
    auto pending = std::make_shared<PendingCalls>();
    pending->remaining = workers;
    pending->cancel = std::vector<boost::asio::cancellation_signal>(workers);
    auto finished = pending->done.get_future();
    size_t nextRow = 0;
    for (size_t worker = 0; worker < workers; ++worker)
    {
        boost::asio::co_spawn(
            strand,
            processRows(worker, *input, tupleBuffer, rows, nextRow),
            boost::asio::bind_cancellation_slot(
                pending->cancel[worker].slot(),
                [pending](const std::exception_ptr& error)
                {
                    if (error && !pending->failure)
                    {
                        pending->failure = error;
                    }
                    if (--pending->remaining == 0)
                    {
                        pending->done.set_value();
                    }
                }));
    }
    {
        /// The signals belong to the strand; the stop request only asks it to emit them.
        const std::stop_callback cancelCalls(
            stopToken,
            [this, pending]
            {
                boost::asio::post(
                    strand,
                    [pending]
                    {
                        for (auto& signal : pending->cancel)
                        {
                            signal.emit(boost::asio::cancellation_type::terminal);
                        }
                    });
            });
        finished.wait();
    }
    if (stopToken.stop_requested())
    {
        return FillTupleBufferResult::eos();
    }
    if (pending->failure)
    {
        std::rethrow_exception(pending->failure);
    }
    return FillTupleBufferResult::withBytes(rows);
}

std::ostream& AsyncSource::toString(std::ostream& str) const
{
    return str << "AsyncSource(" << channelId << ")";
}

DescriptorConfig::Config AsyncSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersAsync>(std::move(config), NAME);
}

}
