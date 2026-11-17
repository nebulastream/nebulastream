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

#include <SinkSession.hpp>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <exception>
#include <expected>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <Configuration/WorkerConfiguration.hpp>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceHandle.hpp>
#include <Util/Overloaded.hpp>
#include <Util/UUID.hpp>
#include <BackpressureChannel.hpp>
#include <Binder.hpp>
#include <BufferPool.hpp>
#include <CompiledQueryPlan.hpp>
#include <ControlChannel.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <QueryEngine.hpp>
#include <QueryEngineConfiguration.hpp>
#include <QueryEngineStatisticListener.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <Reply.hpp>
#include <SessionCommand.hpp>

#include <fmt/format.h>

namespace NES::ConnTest
{
namespace
{

/// The sink is the only pipeline of the session's query.
constexpr PipelineId SINK_PIPELINE = INITIAL_PIPELINE_ID;

/// Records end with a newline, except possibly the last one.
std::vector<std::string_view> splitRecords(const std::string_view bytes)
{
    std::vector<std::string_view> records;
    std::size_t begin = 0;
    while (begin < bytes.size())
    {
        const auto newline = bytes.find('\n', begin);
        const auto end = newline == std::string_view::npos ? bytes.size() : newline + 1;
        records.push_back(bytes.substr(begin, end - begin));
        begin = end;
    }
    return records;
}

/// The batches of records that WRITE releases to the session's source, in order.
class BatchFeed
{
public:
    void release(std::vector<std::string> batches)
    {
        {
            const std::scoped_lock lock(mutex);
            std::ranges::move(batches, std::back_inserter(released));
        }
        changed.notify_all();
    }

    /// Waits for the next released batch, and returns nothing once the source is stopped.
    std::optional<std::string> take(const std::stop_token& stop)
    {
        std::unique_lock lock(mutex);
        if (!changed.wait(lock, stop, [this] { return !released.empty(); }))
        {
            return std::nullopt;
        }
        auto batch = std::move(released.front());
        released.pop_front();
        return batch;
    }

private:
    std::mutex mutex;
    std::condition_variable_any changed;
    std::deque<std::string> released;
};

/// The source of the session's query, which reads the batches that WRITE releases.
/// The engine runs it in a source thread, so the sink's backpressure holds it back as it holds back any source.
class FeedSource final : public Source
{
public:
    explicit FeedSource(std::shared_ptr<BatchFeed> feed) : feed(std::move(feed)) { }

    /// Lays out the batch with the output formatter's own function, so the sink gets the buffers that a query's formatter emits.
    /// The main buffer is filled completely, and the rest continues in child buffers from the pool.
    /// The formatter writes one value at a time, and writing the batch at once gives the same layout,
    /// because once a child exists, every later byte goes into the last child.
    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override
    {
        const auto batch = feed->take(stopToken);
        if (!batch)
        {
            return FillTupleBufferResult::eos();
        }
        const auto bytesInMainBuffer = writeValueToBuffer(
            batch->data(),
            batch->size(),
            tupleBuffer.getBufferSize(),
            &tupleBuffer,
            bufferProvider.get(),
            tupleBuffer.getAvailableMemoryArea<int8_t>().data());
        return FillTupleBufferResult::withBytes(bytesInMainBuffer);
    }

    void open(std::shared_ptr<AbstractBufferProvider> provider) override { bufferProvider = std::move(provider); }

    void close() override { }

protected:
    std::ostream& toString(std::ostream& str) const override { return str << "FeedSource"; }

private:
    std::shared_ptr<BatchFeed> feed;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
};

/// Follows the session's query through the engine's listeners, so that a step can wait until the engine has done its work.
class QueryObserver final : public AbstractQueryStatusListener, public QueryEngineStatisticListener
{
public:
    struct State
    {
        std::optional<QueryStatus> status;
        std::optional<Exception> failure;
        /// A sink task that completes without repeating itself has finished its buffer.
        std::uint64_t completedTasks = 0;
        std::uint64_t repeatedTasks = 0;

        [[nodiscard]] std::uint64_t finishedBuffers() const { return completedTasks - repeatedTasks; }
    };

    /// Forgets the previous query, whose late events are then ignored.
    void track(const QueryId& queryId)
    {
        const std::scoped_lock lock(mutex);
        tracked = queryId;
        state = State{};
    }

    /// Waits until the condition holds or the query has failed, at most until the deadline, and returns the state either way.
    template <typename Condition>
    State waitUntil(const std::chrono::steady_clock::time_point deadline, Condition condition)
    {
        std::unique_lock lock(mutex);
        changed.wait_until(lock, deadline, [&] { return state.failure.has_value() || condition(state); });
        return state;
    }

    bool logSourceTermination(QueryId, OriginId, QueryTerminationType, std::chrono::system_clock::time_point) override { return true; }

    bool logQueryFailure(const QueryId queryId, Exception exception, std::chrono::system_clock::time_point) override
    {
        update(queryId, [&exception](State& current) { current.failure = std::move(exception); });
        return true;
    }

    bool logQueryStatusChange(const QueryId queryId, const QueryStatus status, std::chrono::system_clock::time_point) override
    {
        update(queryId, [status](State& current) { current.status = status; });
        return true;
    }

    void onEvent(Event event) override
    {
        std::visit(
            Overloaded{
                [this](const TaskEmit& emit)
                {
                    if (emit.fromPipeline == SINK_PIPELINE && emit.toPipeline == SINK_PIPELINE)
                    {
                        update(emit.queryId, [](State& current) { ++current.repeatedTasks; });
                    }
                },
                [this](const TaskExecutionComplete& complete)
                {
                    if (complete.pipelineId == SINK_PIPELINE)
                    {
                        update(complete.queryId, [](State& current) { ++current.completedTasks; });
                    }
                },
                [](const auto&) { }},
            event);
    }

private:
    template <typename Change>
    void update(const QueryId& queryId, Change change)
    {
        {
            const std::scoped_lock lock(mutex);
            if (!tracked || queryId != *tracked)
            {
                return;
            }
            change(state);
        }
        changed.notify_all();
    }

    std::mutex mutex;
    std::condition_variable changed;
    std::optional<QueryId> tracked;
    State state;
};

/// The state that persists across the commands of one session.
class SinkSessionState
{
public:
    explicit SinkSessionState(const SinkSessionOptions& options) : options(options) { }

    SinkSessionState(const SinkSessionState&) = delete;
    SinkSessionState& operator=(const SinkSessionState&) = delete;
    SinkSessionState(SinkSessionState&&) = delete;
    SinkSessionState& operator=(SinkSessionState&&) = delete;

    /// A session that ends without STOP destroys the engine with the query still running, which destroys the sink without stopping it.
    /// The runner ends a session that way only after the scenario failed, and a sink that failed may not be able to stop.
    ~SinkSessionState()
    {
        if (stuck)
        {
            exitStuck();
        }
    }

    std::string dispatch(std::string_view line)
    {
        const auto verb = verbOf(line);
        if (verb == "BIND")
        {
            return bind();
        }
        if (verb == "START")
        {
            return start();
        }
        if (verb == "WRITE")
        {
            const auto count = argOf(line);
            if (!count)
            {
                return errorReply(harnessErrorInfo("write", "HarnessProtocol", fmt::format("WRITE needs <n_buffers>: {}", line)));
            }
            return write(*count);
        }
        if (verb == "STOP")
        {
            return stop();
        }
        return errorReply(harnessErrorInfo("dispatch", "HarnessProtocol", fmt::format("unknown command: {}", line)));
    }

private:
    /// An engine thread may still be in the sink after a step timed out, so nothing that it uses can be torn down safely.
    [[noreturn]] static void exitStuck()
    {
        std::cerr << "conntest-harness: the sink did not finish a step within the step timeout, exiting\n";
        std::_Exit(SINK_STUCK_EXIT_CODE);
    }

    std::string bind()
    {
        const auto& descriptor = ensureBound();
        if (!descriptor)
        {
            return errorReply(descriptor.error());
        }
        /// The config's CREATE SINK statement binds to a named sink, whose schema holds the declared columns in order.
        const auto* named = std::get_if<NamedSinkDescriptor>(&descriptor->getUnderlying());
        INVARIANT(named != nullptr, "a CREATE SINK statement binds to a named sink");
        return bindReply(*named->getSchema());
    }

    /// Binds on the first call only, so a repeated BIND or START returns the same descriptor or the same error.
    const std::expected<SinkDescriptor, ErrorInfo>& ensureBound()
    {
        if (!bound)
        {
            bound.emplace(bindConfig());
        }
        return *bound;
    }

    [[nodiscard]] std::expected<SinkDescriptor, ErrorInfo> bindConfig() const
    {
        const auto sql = slurpFile(options.configFile);
        if (!sql)
        {
            return std::unexpected{
                harnessErrorInfo("bind", "HarnessIO", fmt::format("cannot read config file: {}", options.configFile.string()))};
        }
        return bindSinkStatements(*sql);
    }

    /// Every START runs a new query in the engine, with a new sink from the once-bound descriptor and the session's source.
    std::string start()
    {
        const auto& descriptor = ensureBound();
        if (!descriptor)
        {
            return errorReply(descriptor.error());
        }
        if (query)
        {
            return errorReply(harnessErrorInfo("start", "HarnessProtocol", "START while the sink is started"));
        }
        if (auto inputError = ensureInput())
        {
            return errorReply(*inputError);
        }
        if (auto engineError = ensureEngine())
        {
            return errorReply(*engineError);
        }

        query = QueryId::createLocal(LocalQueryId(generateUUID()));
        observer->track(*query);
        feed = std::make_shared<BatchFeed>();
        try
        {
            auto [controller, listener] = createBackpressureChannel();
            auto sink = ExecutablePipeline::create(SINK_PIPELINE, NES::lower(std::move(controller), *descriptor), {});
            std::vector<ExecutableQueryPlan::SourceWithSuccessor> sources;
            sources.emplace_back(
                std::make_unique<SourceHandle>(
                    listener,
                    INITIAL_ORIGIN_ID,
                    SourceRuntimeConfiguration{workerDefaults.defaultMaxInflightBuffers},
                    pool,
                    std::make_unique<FeedSource>(feed)),
                std::vector<std::weak_ptr<ExecutablePipeline>>{sink});
            engine->start(std::make_unique<ExecutableQueryPlan>(*query, std::vector{sink}, std::move(sources), std::move(listener)));
        }
        catch (const Exception& ex)
        {
            query.reset();
            return errorReply(connectorErrorInfo("start", ex));
        }
        if (auto error = awaitStep("start", [](const QueryObserver::State& state) { return state.status == QueryStatus::Running; }))
        {
            query.reset();
            return errorReply(*error);
        }
        return startReply(batches.size());
    }

    /// The pool and the engine are made once, at the first START, because the pool's size depends on the input.
    std::optional<ErrorInfo> ensureEngine()
    {
        if (engine)
        {
            return std::nullopt;
        }
        try
        {
            /// A worker's pool is far larger than one test's data, so the source never waits for a buffer that the sink still holds.
            /// A batch takes its main buffer and a child for each further buffer size of bytes.
            std::size_t bufferCount = 0;
            for (const auto& batch : batches)
            {
                bufferCount += (batch.size() + options.bufferSize - 1) / options.bufferSize;
            }
            pool = makeBufferPool(
                static_cast<std::uint32_t>(options.bufferSize), static_cast<std::uint32_t>(std::max<std::size_t>(bufferCount, 1)));
            engine = std::make_unique<QueryEngine>(
                QueryEngineConfiguration{std::max<std::size_t>(options.threads, 1), workerDefaults.queryEngine.admissionQueueSize},
                observer,
                observer,
                pool,
                Host("conntest-harness"));
        }
        catch (const std::exception& ex)
        {
            return harnessErrorInfo("start", "HarnessIO", ex.what());
        }
        return std::nullopt;
    }

    /// The runner writes the input after BIND, so the session reads it at the first START.
    /// Every START writes the batches from the first one again.
    std::optional<ErrorInfo> ensureInput()
    {
        nextBatch = 0;
        releasedBatches = 0;
        if (inputRead || !options.inputPath)
        {
            return std::nullopt;
        }
        const auto bytes = slurpFile(*options.inputPath);
        if (!bytes)
        {
            return harnessErrorInfo("start", "HarnessIO", fmt::format("cannot read input file: {}", options.inputPath->string()));
        }
        packRecords(splitRecords(*bytes));
        inputRead = true;
        return std::nullopt;
    }

    /// Packs the records as a query's emit operator does, which writes records into a buffer until its main buffer is full.
    /// The record that crosses the end of the main buffer continues in a child buffer, so no record is split between two buffers.
    /// With 4096-byte buffers and records of 1000 bytes, a batch holds five records, of which the fifth puts 904 bytes into a child.
    void packRecords(const std::vector<std::string_view>& records)
    {
        std::string batch;
        for (const auto record : records)
        {
            if (batch.size() >= options.bufferSize)
            {
                batches.push_back(std::exchange(batch, {}));
            }
            batch.append(record);
        }
        if (!batch.empty())
        {
            batches.push_back(std::move(batch));
        }
    }

    /// Releases the next batches that START made, or as many as are left, and returns once the sink has finished them.
    std::string write(const std::uint64_t numberOfBuffers)
    {
        if (!query)
        {
            return errorReply(harnessErrorInfo("write", "HarnessProtocol", "WRITE without a started sink"));
        }
        const auto count = std::min<std::uint64_t>(numberOfBuffers, batches.size() - nextBatch);
        std::vector<std::string> step(
            batches.begin() + static_cast<std::ptrdiff_t>(nextBatch), batches.begin() + static_cast<std::ptrdiff_t>(nextBatch + count));
        nextBatch += count;
        releasedBatches += count;
        feed->release(std::move(step));
        const auto released = releasedBatches;
        if (auto error = awaitStep("write", [released](const QueryObserver::State& state) { return state.finishedBuffers() >= released; }))
        {
            return errorReply(*error);
        }
        return okReply("write");
    }

    /// Stops the query as a user does, after which the engine stops the sink once it has finished every buffer that it received.
    std::string stop()
    {
        if (!query)
        {
            return errorReply(harnessErrorInfo("stop", "HarnessProtocol", "STOP without a started sink"));
        }
        engine->stop(*query);
        auto error = awaitStep("stop", [](const QueryObserver::State& state) { return state.status == QueryStatus::Stopped; });
        query.reset();
        if (error)
        {
            return errorReply(*error);
        }
        return okReply("stop");
    }

    /// Waits until the engine has done the step's work, within the step timeout.
    /// A failed query fails the step with the sink's error, which the engine reports unchanged.
    template <typename Done>
    std::optional<ErrorInfo> awaitStep(const std::string_view phase, Done done)
    {
        const auto state = observer->waitUntil(std::chrono::steady_clock::now() + STEP_TIMEOUT, done);
        if (state.failure)
        {
            return connectorErrorInfo(phase, *state.failure);
        }
        if (!done(state))
        {
            stuck = true;
            return harnessErrorInfo(phase, "StepTimeout", fmt::format("{} timed out after {} ms", phase, STEP_TIMEOUT.count()));
        }
        return std::nullopt;
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): non-owning view into runSinkSession's options; the state's lifetime is strictly contained by the loop's.
    const SinkSessionOptions& options;
    /// A worker's defaults for what a configuration of the engine and a source can set.
    WorkerConfiguration workerDefaults = defaultConfiguration<WorkerConfiguration>();
    std::optional<std::expected<SinkDescriptor, ErrorInfo>> bound;
    /// The batches that the first START made from the input, the next one that WRITE releases, and how many it released.
    bool inputRead = false;
    std::vector<std::string> batches;
    std::size_t nextBatch = 0;
    std::uint64_t releasedBatches = 0;
    std::shared_ptr<BatchFeed> feed;
    std::shared_ptr<QueryObserver> observer = std::make_shared<QueryObserver>();
    std::shared_ptr<BufferManager> pool;
    /// The running query, from START until STOP.
    std::optional<QueryId> query;
    /// Set once a step timed out.
    bool stuck = false;
    /// Declared last, so that the engine stops its threads before the members that they use are destroyed.
    std::unique_ptr<QueryEngine> engine;
};

}

bool runSinkSession(const SinkSessionOptions& options, ControlChannel& channel)
{
    SinkSessionState state(options);
    while (true)
    {
        const auto line = channel.readLine();
        if (!line)
        {
            return true;
        }
        if (line->empty())
        {
            continue; /// Blank keep-alive line.
        }
        if (!channel.writeLine(state.dispatch(*line)))
        {
            return false;
        }
    }
}

}
