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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/AsyncFunction.hpp>
#include <Sources/AsyncResourceLimiter.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/UUID.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <AsyncFunctionRegistry.hpp>
#include <AsyncScheduling.hpp>
#include <AsyncSource.hpp>
#include <ErrorHandling.hpp>
#include <HandoffChannel.hpp>

namespace NES
{
namespace
{

/// --- StreamReorderer ---

StreamPosition at(const uint64_t origin, const uint64_t sequence, const uint64_t chunk = 1, const bool last = true)
{
    return {.origin = origin, .sequence = sequence, .chunk = chunk, .lastChunk = last};
}

TEST(StreamReordererTest, PassesBuffersThatArriveInOrder)
{
    StreamReorderer<int> reorderer;
    EXPECT_EQ(reorderer.push(at(1, 1), 1), std::vector{1});
    EXPECT_EQ(reorderer.push(at(1, 2), 2), std::vector{2});
    EXPECT_EQ(reorderer.size(), 0);
}

TEST(StreamReordererTest, HoldsABufferUntilItsPredecessorArrives)
{
    StreamReorderer<int> reorderer;
    EXPECT_TRUE(reorderer.push(at(1, 3), 3).empty());
    EXPECT_TRUE(reorderer.push(at(1, 2), 2).empty());
    EXPECT_EQ(reorderer.size(), 2);
    EXPECT_EQ(reorderer.push(at(1, 1), 1), (std::vector{1, 2, 3}));
    EXPECT_EQ(reorderer.size(), 0);
}

TEST(StreamReordererTest, OrdersChunksAndMovesOnAfterTheLastChunk)
{
    StreamReorderer<std::string> reorderer;
    EXPECT_TRUE(reorderer.push(at(1, 2, 1, true), "2.1").empty());
    EXPECT_TRUE(reorderer.push(at(1, 1, 3, true), "1.3").empty());
    EXPECT_EQ(reorderer.push(at(1, 1, 1, false), "1.1"), std::vector<std::string>{"1.1"});
    EXPECT_EQ(reorderer.push(at(1, 1, 2, false), "1.2"), (std::vector<std::string>{"1.2", "1.3", "2.1"}));
}

TEST(StreamReordererTest, OriginsDoNotWaitForEachOther)
{
    StreamReorderer<std::string> reorderer;
    EXPECT_TRUE(reorderer.push(at(1, 2), "a2").empty());
    EXPECT_EQ(reorderer.push(at(2, 1), "b1"), std::vector<std::string>{"b1"});
    EXPECT_EQ(reorderer.push(at(1, 1), "a1"), (std::vector<std::string>{"a1", "a2"}));
}

TEST(StreamReordererTest, ReleasesAPositionThatIsAlreadyBehind)
{
    StreamReorderer<int> reorderer;
    EXPECT_EQ(reorderer.push(at(1, 1), 1), std::vector{1});
    EXPECT_EQ(reorderer.push(at(1, 1), 10), std::vector{10});
}

TEST(StreamReordererTest, DrainsWhatIsLeftInStreamOrder)
{
    StreamReorderer<int> reorderer;
    EXPECT_TRUE(reorderer.push(at(2, 5), 25).empty());
    EXPECT_TRUE(reorderer.push(at(1, 4), 14).empty());
    EXPECT_TRUE(reorderer.push(at(1, 3), 13).empty());
    EXPECT_EQ(reorderer.drain(), (std::vector{13, 14, 25}));
    EXPECT_EQ(reorderer.size(), 0);
}

TEST(StreamReordererTest, RestoresAShuffledStreamOfManyChunkedSequences)
{
    std::mt19937 random{42};
    std::vector<std::pair<StreamPosition, std::string>> stream;
    for (uint64_t origin = 1; origin <= 3; ++origin)
    {
        for (uint64_t sequence = 1; sequence <= 50; ++sequence)
        {
            const auto chunks = 1 + (random() % 4);
            for (uint64_t chunk = 1; chunk <= chunks; ++chunk)
            {
                stream.emplace_back(at(origin, sequence, chunk, chunk == chunks), fmt::format("{}/{}/{}", origin, sequence, chunk));
            }
        }
    }
    auto shuffled = stream;
    std::ranges::shuffle(shuffled, random);
    StreamReorderer<std::string> reorderer;
    std::map<uint64_t, std::vector<std::string>> released;
    for (auto& [position, item] : shuffled)
    {
        for (auto& next : reorderer.push(position, item))
        {
            released[std::stoull(next.substr(0, next.find('/')))].push_back(next);
        }
    }
    EXPECT_EQ(reorderer.size(), 0);
    for (uint64_t origin = 1; origin <= 3; ++origin)
    {
        std::vector<std::string> expected;
        for (const auto& [position, item] : stream)
        {
            if (position.origin == origin)
            {
                expected.push_back(item);
            }
        }
        EXPECT_EQ(released[origin], expected);
    }
}

/// --- KeyedRowQueue ---

std::vector<int> rowsOf(const std::vector<std::pair<int, std::vector<std::string>>>& taken)
{
    std::vector<int> rows;
    for (const auto& [row, keys] : taken)
    {
        rows.push_back(row);
    }
    return rows;
}

TEST(KeyedRowQueueTest, FormsBatchesOfReadyRowsInStreamOrder)
{
    KeyedRowQueue<int> queue;
    for (int row = 0; row < 6; ++row)
    {
        queue.push(row, {});
    }
    EXPECT_EQ(queue.startableRows(), 6);
    EXPECT_EQ(rowsOf(queue.take(4)), (std::vector{0, 1, 2, 3}));
    /// The partial last batch: whatever is left.
    EXPECT_EQ(rowsOf(queue.take(4)), (std::vector{4, 5}));
    EXPECT_TRUE(queue.take(4).empty());
}

TEST(KeyedRowQueueTest, RunsOneRowPerKeyAtATimeInStreamOrder)
{
    KeyedRowQueue<int> queue;
    queue.push(1, {"a"});
    queue.push(2, {"a"});
    queue.push(3, {"b"});
    queue.push(4, {"a"});
    /// One per key, however large the batch may be.
    const auto first = queue.take(10);
    EXPECT_EQ(rowsOf(first), (std::vector{1, 3}));
    EXPECT_EQ(queue.startableRows(), 0);
    EXPECT_TRUE(queue.take(10).empty());
    queue.finish({"a"});
    EXPECT_EQ(rowsOf(queue.take(10)), std::vector{2});
    queue.finish({"b"});
    EXPECT_TRUE(queue.take(10).empty());
    queue.finish({"a"});
    EXPECT_EQ(rowsOf(queue.take(10)), std::vector{4});
}

TEST(KeyedRowQueueTest, WaitingRowsHoldNoSlot)
{
    /// Two slots, one row each: while key a is busy, the second slot takes the next key rather than idling.
    KeyedRowQueue<int> queue;
    queue.push(1, {"a"});
    queue.push(2, {"a"});
    queue.push(3, {"a"});
    queue.push(4, {"b"});
    EXPECT_EQ(rowsOf(queue.take(1)), std::vector{1});
    EXPECT_EQ(rowsOf(queue.take(1)), std::vector{4});
    EXPECT_EQ(queue.size(), 2);
    EXPECT_EQ(queue.runningKeys(), 2);
}

TEST(KeyedRowQueueTest, ARowNeverOvertakesAnEarlierRowOfOneOfItsKeys)
{
    KeyedRowQueue<int> queue;
    queue.push(1, {"a"});
    queue.push(2, {"a", "b"});
    queue.push(3, {"b"});
    queue.push(4, {});
    /// Row 3's key b is free, but row 2 is waiting with b ahead of it. Rows without keys are never held back.
    EXPECT_EQ(rowsOf(queue.take(10)), (std::vector{1, 4}));
    queue.finish({"a"});
    EXPECT_EQ(rowsOf(queue.take(10)), std::vector{2});
    queue.finish({"a", "b"});
    EXPECT_EQ(rowsOf(queue.take(10)), std::vector{3});
}

/// --- AsyncSource with test functions ---

/// What the test functions saw, shared by all instances of a test.
TEST(RetryBudgetTest, RetriesOtherFailuresMaxRetriesTimesWithDoublingBackoff)
{
    const auto now = std::chrono::steady_clock::now();
    RetryBudget budget{RetryPolicy{.maxRetries = 2}, now};
    EXPECT_EQ(budget.next(false, std::nullopt, now, 0.5), std::chrono::milliseconds{100});
    EXPECT_EQ(budget.next(false, std::nullopt, now, 0.5), std::chrono::milliseconds{200});
    EXPECT_EQ(budget.next(false, std::nullopt, now, 0.5), std::nullopt);
}

TEST(RetryBudgetTest, BacksOffTransientFailuresExponentiallyWithJitterUpToTheCap)
{
    const auto now = std::chrono::steady_clock::now();
    const RetryPolicy policy{.maxRetries = 0, .maxTransientRetries = 6, .maxBackoff = std::chrono::milliseconds{1000}};
    RetryBudget low{policy, now};
    RetryBudget high{policy, now};
    /// d = 100, 200, 400, 800, then capped at 1000: the wait lies in [d/2, d].
    for (const auto span : {100, 200, 400, 800, 1000, 1000})
    {
        EXPECT_EQ(low.next(true, std::nullopt, now, 0.0), std::chrono::milliseconds{span / 2});
        EXPECT_EQ(high.next(true, std::nullopt, now, 1.0), std::chrono::milliseconds{span});
    }
    EXPECT_EQ(low.next(true, std::nullopt, now, 0.0), std::nullopt) << "only maxTransientRetries retries";
}

TEST(RetryBudgetTest, WaitsAtLeastWhatTheServiceAskedFor)
{
    const auto now = std::chrono::steady_clock::now();
    RetryBudget budget{RetryPolicy{}, now};
    EXPECT_EQ(budget.next(true, std::chrono::milliseconds{2500}, now, 0.0), std::chrono::milliseconds{2500});
    EXPECT_EQ(budget.next(true, std::chrono::milliseconds{10}, now, 0.0), std::chrono::milliseconds{100}) << "the backoff, if longer";
}

TEST(RetryBudgetTest, GivesUpWhenTheNextAttemptWouldComeAfterTheTimeLimit)
{
    const auto start = std::chrono::steady_clock::now();
    RetryBudget budget{RetryPolicy{.transientTimeLimit = std::chrono::seconds{10}}, start};
    EXPECT_TRUE(budget.next(true, std::chrono::seconds{9}, start, 0.0).has_value());
    EXPECT_EQ(budget.next(true, std::chrono::seconds{2}, start + std::chrono::seconds{9}, 0.0), std::nullopt);
    RetryBudget asked{RetryPolicy{.transientTimeLimit = std::chrono::seconds{10}}, start};
    EXPECT_EQ(asked.next(true, std::chrono::seconds{11}, start, 0.0), std::nullopt) << "a Retry-After beyond the limit fails at once";
}

TEST(RetryBudgetTest, TransientFailuresDoNotUseUpTheRetriesOfOtherFailures)
{
    const auto now = std::chrono::steady_clock::now();
    RetryBudget budget{RetryPolicy{.maxRetries = 1, .maxTransientRetries = 3}, now};
    for (int retry = 0; retry < 3; ++retry)
    {
        EXPECT_TRUE(budget.next(true, std::nullopt, now, 0.0).has_value());
    }
    EXPECT_TRUE(budget.next(false, std::nullopt, now, 0.0).has_value());
    EXPECT_EQ(budget.next(false, std::nullopt, now, 0.0), std::nullopt);
    EXPECT_EQ(budget.next(true, std::nullopt, now, 0.0), std::nullopt);
}

struct Observations
{
    std::mutex mutex;
    /// Rows per request, in the order the requests were made.
    std::vector<size_t> requests;
    /// Per key, the values in the order their calls started.
    std::map<std::string, std::vector<std::string>> startedPerKey;
    std::set<std::string> keysInFlight;
    size_t overlapsOfOneKey = 0;
    size_t inFlight = 0;
    size_t mostInFlight = 0;
    /// Batches that still fail before the function answers; see `ScriptedFunction`.
    size_t failingBatches = 0;
    size_t failingRows = 0;
    /// Requests that still fail transiently (`AsyncTransientFailure`), with this `retryAfter`.
    size_t transientFailures = 0;
    std::optional<std::chrono::milliseconds> retryAfter;
    /// When each request started, and the retries the source announced (delay, transient).
    std::vector<std::chrono::steady_clock::time_point> requestStarts;
    std::vector<std::pair<std::chrono::milliseconds, bool>> retries;
};

Observations observed; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables) shared with the registered factories

/// Configured with "<batch>,<keyed>,<delay ms>,<omit>": requests take `batch` rows, the first argument orders the calls
/// if `keyed` is 1, each request waits `delay` ms (plus a bit per value, so that requests finish out of order), and a
/// batch leaves out its last row's result if `omit` is 1. Its value is the second argument followed by '!'.
class ScriptedFunction final : public AsyncFunction
{
public:
    explicit ScriptedFunction(const std::string& configuration)
    {
        std::array<char, 64> resourceName{};
        /// NOLINTNEXTLINE(cert-err34-c)
        if (std::sscanf(
                configuration.c_str(), "%zu,%d,%d,%d,%63[^,],%zu", &batch, &keyed, &delayMs, &omit, resourceName.data(), &sharedBound)
            == 6)
        {
            resource = resourceName.data();
        }
    }

    [[nodiscard]] std::optional<AsyncSharedLimit> sharedLimit() const override
    {
        return resource.empty() ? std::nullopt : std::optional{AsyncSharedLimit{.resource = resource, .limit = sharedBound}};
    }

    boost::asio::awaitable<AsyncCallResult> callValues(std::vector<std::string> arguments) override
    {
        auto results = co_await run({std::move(arguments)}, false);
        co_return std::move(*results.front());
    }

    boost::asio::awaitable<AsyncBatchResult> callBatch(std::vector<std::vector<std::string>> rows) override
    {
        co_return co_await run(std::move(rows), true);
    }

    [[nodiscard]] size_t maxBatchSize() const override { return batch; }

    void retryScheduled(const std::chrono::milliseconds delay, const bool transient) override
    {
        const std::scoped_lock lock(observed.mutex);
        observed.retries.emplace_back(delay, transient);
    }

    [[nodiscard]] std::optional<size_t> orderingKey() const override { return keyed == 1 ? std::optional<size_t>{0} : std::nullopt; }

private:
    boost::asio::awaitable<AsyncBatchResult> run(std::vector<std::vector<std::string>> rows, const bool asBatch)
    {
        bool fail = false;
        bool failTransiently = false;
        {
            const std::scoped_lock lock(observed.mutex);
            observed.requests.push_back(rows.size());
            observed.requestStarts.push_back(std::chrono::steady_clock::now());
            if (observed.transientFailures > 0)
            {
                --observed.transientFailures;
                failTransiently = true;
            }
            observed.mostInFlight = std::max(observed.mostInFlight, ++observed.inFlight);
            for (const auto& row : rows)
            {
                if (keyed == 1)
                {
                    observed.overlapsOfOneKey += observed.keysInFlight.contains(row[0]) ? 1 : 0;
                    observed.keysInFlight.insert(row[0]);
                    observed.startedPerKey[row[0]].push_back(row[1]);
                }
            }
            auto& failing = asBatch ? observed.failingBatches : observed.failingRows;
            if (failing > 0)
            {
                --failing;
                fail = true;
            }
        }
        const auto jitter = std::hash<std::string>{}(rows.front()[1]) % 3;
        boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, std::chrono::milliseconds(delayMs + jitter)};
        co_await timer.async_wait(boost::asio::use_awaitable);
        {
            const std::scoped_lock lock(observed.mutex);
            --observed.inFlight;
            for (const auto& row : rows)
            {
                observed.keysInFlight.erase(row[0]);
            }
        }
        if (failTransiently)
        {
            throw AsyncTransientFailure(CannotReadFromSource("scripted transient failure"), observed.retryAfter);
        }
        if (fail)
        {
            throw CannotReadFromSource("scripted failure");
        }
        AsyncBatchResult results;
        for (const auto& row : rows)
        {
            results.emplace_back(AsyncCallResult{row[1] + "!"});
        }
        if (asBatch && omit == 1)
        {
            results.back().reset();
        }
        co_return results;
    }

    size_t batch = 1;
    int keyed = 0;
    int delayMs = 0;
    int omit = 0;
    /// "<batch>,<keyed>,<delay ms>,<omit>,<resource>,<limit>": calls share a limit of `limit` with all calls to `resource`.
    std::string resource;
    size_t sharedBound = 0;
};

/// An Async source over rows (key, value, result slot), fed through its channel like a Handoff sink would.
class AsyncSourceTest : public testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        std::ignore = AsyncFunctionRegistry::instance().addEntry(
            "ScriptedTest",
            [](AsyncFunctionRegistryArguments arguments) -> AsyncFunctionRegistryReturnType
            { return std::make_unique<ScriptedFunction>(arguments.configuration); });
    }

    void SetUp() override
    {
        observed.requests.clear();
        observed.startedPerKey.clear();
        observed.keysInFlight.clear();
        observed.overlapsOfOneKey = 0;
        observed.inFlight = 0;
        observed.mostInFlight = 0;
        observed.failingBatches = 0;
        observed.failingRows = 0;
        observed.transientFailures = 0;
        observed.retryAfter.reset();
        observed.requestStarts.clear();
        observed.retries.clear();

        const auto schema = Schema<UnqualifiedUnboundField, Ordered>{
            UnqualifiedUnboundField{Identifier::parse("key"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("value"), DataType::Type::VARSIZED},
            UnqualifiedUnboundField{Identifier::parse("result"), DataType::Type::VARSIZED}};
        const auto logical = catalog.addLogicalSource(Identifier::parse("handedOff"), schema);
        ASSERT_TRUE(logical.has_value());
        auto physical = catalog.addPhysicalSource(
            *logical,
            Identifier::parse("FILE"),
            Host{"localhost:8080"},
            {{Identifier::parse("FILE_PATH"), "'unused'"}},
            {{Identifier::parse("TYPE"), "CSV"}});
        ASSERT_TRUE(physical.has_value()) << physical.error();
        descriptor.emplace(std::move(*physical));
        for (const auto& field : descriptor->getSchema())
        {
            names.push_back(fmt::format("{}", field.getFullyQualifiedName()));
        }
    }

    void startSource(
        const std::string& functionConfiguration,
        const size_t concurrency,
        const size_t retries,
        const size_t transientRetries = AsyncSourceConfig{}.transientRetries,
        const size_t transientRetrySeconds = AsyncSourceConfig{}.transientRetrySeconds,
        const size_t readAheadRows = 0)
    {
        source = makeSource(channelId, functionConfiguration, concurrency, retries, transientRetries, transientRetrySeconds, readAheadRows);
        sink.emplace(channelId, 1024, HandoffChannel::Role::SINK);
        source->open(bufferManager);
    }

    std::unique_ptr<AsyncSource> makeSource(
        const std::string& channel,
        const std::string& functionConfiguration,
        const size_t concurrency,
        const size_t retries = 0,
        const size_t transientRetries = AsyncSourceConfig{}.transientRetries,
        const size_t transientRetrySeconds = AsyncSourceConfig{}.transientRetrySeconds,
        const size_t readAheadRows = 0)
    {
        const auto calls = fmt::format(
            R"([{{"outputs":[{{"index":0,"field":"{}"}}],"function":"ScriptedTest","configuration":"{}","arguments":[{{"field":"{}","output":0}},{{"field":"{}","output":0}}]}}])",
            names[2],
            functionConfiguration,
            names[0],
            names[1]);
        const AsyncSourceConfig config{
            .channel = channel,
            .calls = calls,
            .inputFields = fmt::format("{},{},{}", names[0], names[1], names[2]),
            .maxConcurrency = concurrency,
            .channelCapacity = 1024,
            .maxRetries = retries,
            .transientRetries = transientRetries,
            .transientRetrySeconds = transientRetrySeconds,
            .readAheadRows = readAheadRows};
        return std::make_unique<AsyncSource>(config, *descriptor);
    }

    /// One handed-off buffer of (key, value) rows at the given stream position.
    TupleBuffer handedOff(const StreamPosition& position, const std::vector<std::pair<std::string, std::string>>& rows)
    {
        auto buffer = bufferManager->getBufferBlocking();
        constexpr auto fieldSize = sizeof(VariableSizedAccess);
        const auto memory = buffer.getAvailableMemoryArea();
        for (size_t row = 0; row < rows.size(); ++row)
        {
            for (size_t field = 0; field < 3; ++field)
            {
                const std::string value = field == 0 ? rows[row].first : field == 1 ? rows[row].second : "";
                const auto access = TupleBufferRef::writeVarSized(buffer, *bufferManager, std::as_bytes(std::span{value}));
                std::memcpy(memory.subspan(((row * 3) + field) * fieldSize).data(), &access, fieldSize);
            }
        }
        buffer.setNumberOfTuples(rows.size());
        buffer.setOriginId(OriginId(position.origin));
        buffer.setSequenceNumber(SequenceNumber(position.sequence));
        buffer.setChunkNumber(ChunkNumber(position.chunk));
        buffer.setLastChunk(position.lastChunk);
        return buffer;
    }

    void handOff(const TupleBuffer& buffer)
    {
        while ((*sink)->tryPush(buffer) != HandoffChannel::PushResult::PUSHED)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    /// Per emitted row its origin, value and result, until the end of the stream.
    std::vector<std::tuple<uint64_t, std::string, std::string>> drain() { return drain(*source); }

    std::vector<std::tuple<uint64_t, std::string, std::string>> drain(AsyncSource& from)
    {
        std::vector<std::tuple<uint64_t, std::string, std::string>> rows;
        while (true)
        {
            auto output = bufferManager->getBufferBlocking();
            const auto result = from.fillTupleBuffer(output, std::stop_token{});
            if (result.isEoS())
            {
                break;
            }
            constexpr auto fieldSize = sizeof(VariableSizedAccess);
            const auto memory = output.getAvailableMemoryArea();
            for (size_t row = 0; row < result.getNumberOfBytes(); ++row)
            {
                std::vector<std::string> values;
                for (size_t field = 1; field < 3; ++field)
                {
                    VariableSizedAccess access;
                    std::memcpy(&access, memory.subspan(((row * 3) + field) * fieldSize).data(), fieldSize);
                    const auto bytes = TupleBufferRef::loadAssociatedVarSizedValue(output, access);
                    values.emplace_back(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                }
                rows.emplace_back(output.getOriginId().getRawValue(), values[0], values[1]);
            }
        }
        from.close();
        return rows;
    }

    void handOff(HandoffChannel::Lease& to, const TupleBuffer& buffer)
    {
        while (to->tryPush(buffer) != HandoffChannel::PushResult::PUSHED)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(64 * 1024 * 1024, 0.5, BufferAlignment{64}, 4096, std::make_shared<NesDefaultMemoryAllocator>());
    SourceCatalog catalog;
    std::optional<SourceDescriptor> descriptor;
    std::vector<std::string> names;
    std::string channelId = UUIDToString(generateUUID());
    std::unique_ptr<AsyncSource> source;
    std::optional<HandoffChannel::Lease> sink;
};

/// Four producer threads hand off the buffers of two origins out of order, as the Handoff sink's worker threads do.
TEST_F(AsyncSourceTest, EmitsInStreamOrderWhatSeveralThreadsHandedOffOutOfOrder)
{
    startSource("1,0,1,0", 8, 0);
    std::vector<TupleBuffer> buffers;
    std::map<uint64_t, std::vector<std::string>> expected;
    for (uint64_t origin = 1; origin <= 2; ++origin)
    {
        for (uint64_t sequence = 1; sequence <= 40; ++sequence)
        {
            for (uint64_t chunk = 1; chunk <= 2; ++chunk)
            {
                const auto value = fmt::format("{}-{}-{}", origin, sequence, chunk);
                buffers.push_back(handedOff(at(origin, sequence, chunk, chunk == 2), {{"k", value + "a"}, {"k", value + "b"}}));
                expected[origin].push_back(value + "a");
                expected[origin].push_back(value + "b");
            }
        }
    }
    std::mt19937 random{7};
    std::ranges::shuffle(buffers, random);
    std::vector<std::jthread> producers;
    for (size_t thread = 0; thread < 4; ++thread)
    {
        producers.emplace_back(
            [&, thread]
            {
                for (size_t index = thread; index < buffers.size(); index += 4)
                {
                    handOff(buffers[index]);
                }
            });
    }
    std::jthread closer{[&]
                        {
                            for (auto& producer : producers)
                            {
                                producer.join();
                            }
                            (*sink)->close();
                        }};
    const auto rows = drain();
    std::map<uint64_t, std::vector<std::string>> emitted;
    for (const auto& [origin, value, result] : rows)
    {
        emitted[origin].push_back(value);
        EXPECT_EQ(result, value + "!");
    }
    EXPECT_EQ(emitted, expected);
    EXPECT_GT(observed.mostInFlight, 1);
}

TEST_F(AsyncSourceTest, BatchesReadyRowsAndSendsThePartialLastBatch)
{
    /// One slot: the 10 rows of one buffer go out as 4 + 4 + 2.
    startSource("4,0,1,0", 1, 0);
    std::vector<std::pair<std::string, std::string>> rows;
    for (int row = 0; row < 10; ++row)
    {
        rows.emplace_back("k", fmt::format("v{}", row));
    }
    handOff(handedOff(at(1, 1), rows));
    (*sink)->close();
    const auto emitted = drain();
    ASSERT_EQ(emitted.size(), 10);
    for (int row = 0; row < 10; ++row)
    {
        EXPECT_EQ(std::get<2>(emitted[row]), fmt::format("v{}!", row));
    }
    EXPECT_EQ(observed.requests, (std::vector<size_t>{4, 4, 2}));
}

TEST_F(AsyncSourceTest, BatchesAcrossBufferBoundaries)
{
    /// The calls are slow enough that all buffers are in the window before the single slot frees up again.
    startSource("8,0,20,0", 1, 0);
    for (uint64_t sequence = 1; sequence <= 5; ++sequence)
    {
        handOff(handedOff(at(1, sequence), {{"k", fmt::format("{}a", sequence)}, {"k", fmt::format("{}b", sequence)}}));
    }
    (*sink)->close();
    const auto emitted = drain();
    ASSERT_EQ(emitted.size(), 10);
    EXPECT_EQ(std::get<1>(emitted.back()), "5b");
    size_t total = 0;
    for (const auto rows : observed.requests)
    {
        total += rows;
    }
    EXPECT_EQ(total, 10);
    EXPECT_LT(observed.requests.size(), 5) << "rows of several buffers should share a request";
}

TEST_F(AsyncSourceTest, TakesTheNextBufferAsSoonAsTheSlotsCouldRunOutOfRows)
{
    /// Four slots and buffers of five rows: after the first round only one row of a buffer is left, so the next buffer
    /// has to be taken right then, not once the whole buffer is done. 15 rows are then 4 rounds of 100 ms; waiting for
    /// each buffer to finish first takes 2 rounds per buffer, 6 in all.
    startSource("1,0,100,0", 4, 0);
    const auto start = std::chrono::steady_clock::now();
    for (uint64_t sequence = 1; sequence <= 3; ++sequence)
    {
        std::vector<std::pair<std::string, std::string>> rows;
        for (int row = 0; row < 5; ++row)
        {
            rows.emplace_back("k", fmt::format("{}-{}", sequence, row));
        }
        handOff(handedOff(at(1, sequence), rows));
    }
    (*sink)->close();
    const auto emitted = drain();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    ASSERT_EQ(emitted.size(), 15);
    EXPECT_EQ(std::get<1>(emitted.back()), "3-4");
    EXPECT_LT(elapsed, std::chrono::milliseconds(520)) << "slots idled while the next buffer waited in the channel";
}

TEST_F(AsyncSourceTest, RetriesAFailedBatchThenCallsItsRowsOneByOne)
{
    startSource("3,0,1,0", 1, 1);
    /// The batch fails twice, once more than the one retry: its rows are then called on their own, and the first of
    /// those fails once and is retried.
    observed.failingBatches = 2;
    observed.failingRows = 1;
    handOff(handedOff(at(1, 1), {{"k", "a"}, {"k", "b"}, {"k", "c"}}));
    (*sink)->close();
    const auto emitted = drain();
    ASSERT_EQ(emitted.size(), 3);
    EXPECT_EQ(std::get<2>(emitted[0]), "a!");
    EXPECT_EQ(std::get<2>(emitted[2]), "c!");
    EXPECT_EQ(observed.requests, (std::vector<size_t>{3, 3, 1, 1, 1, 1}));
}

TEST_F(AsyncSourceTest, CallsARowTheBatchLeftOutOnItsOwn)
{
    startSource("3,0,1,1", 1, 0);
    handOff(handedOff(at(1, 1), {{"k", "a"}, {"k", "b"}, {"k", "c"}}));
    (*sink)->close();
    const auto emitted = drain();
    ASSERT_EQ(emitted.size(), 3);
    EXPECT_EQ(std::get<2>(emitted[2]), "c!");
    EXPECT_EQ(observed.requests, (std::vector<size_t>{3, 1}));
}

TEST_F(AsyncSourceTest, RunsOneCallPerKeyAtATimeInStreamOrderAndKeysInParallel)
{
    startSource("1,1,3,0", 4, 0);
    std::map<std::string, std::vector<std::string>> expected;
    std::vector<TupleBuffer> buffers;
    for (uint64_t sequence = 1; sequence <= 20; ++sequence)
    {
        std::vector<std::pair<std::string, std::string>> rows;
        for (const auto* key : {"hot", "hot", "a", "b"})
        {
            const auto value = fmt::format("{}{}-{}", key, sequence, rows.size());
            rows.emplace_back(key, value);
            expected[key].push_back(value);
        }
        buffers.push_back(handedOff(at(1, sequence), rows));
    }
    std::mt19937 random{3};
    std::ranges::shuffle(buffers, random);
    for (const auto& buffer : buffers)
    {
        handOff(buffer);
    }
    (*sink)->close();
    const auto emitted = drain();
    EXPECT_EQ(emitted.size(), 80);
    EXPECT_EQ(observed.overlapsOfOneKey, 0);
    EXPECT_EQ(observed.startedPerKey, expected);
    /// The hot key does not hold the slots: the other keys ran next to it.
    EXPECT_GT(observed.mostInFlight, 1);
}

TEST_F(AsyncSourceTest, BatchesHoldAtMostOneRowPerKey)
{
    startSource("4,1,1,0", 1, 0);
    handOff(handedOff(at(1, 1), {{"a", "a1"}, {"a", "a2"}, {"b", "b1"}, {"c", "c1"}, {"b", "b2"}}));
    (*sink)->close();
    const auto emitted = drain();
    ASSERT_EQ(emitted.size(), 5);
    EXPECT_EQ(observed.overlapsOfOneKey, 0);
    EXPECT_EQ(observed.requests, (std::vector<size_t>{3, 2}));
    EXPECT_EQ(observed.startedPerKey["a"], (std::vector<std::string>{"a1", "a2"}));
    EXPECT_EQ(observed.startedPerKey["b"], (std::vector<std::string>{"b1", "b2"}));
}

TEST_F(AsyncSourceTest, RetriesTransientFailuresAfterTheServicesRetryAfterWithoutMaxRetries)
{
    /// No retries for other failures: a service that answers "too many requests" twice still does not fail the query.
    startSource("1,0,1,0", 1, 0);
    observed.transientFailures = 2;
    observed.retryAfter = std::chrono::milliseconds{150};
    handOff(handedOff(at(1, 1), {{"k", "a"}}));
    (*sink)->close();
    const auto emitted = drain();
    ASSERT_EQ(emitted.size(), 1);
    EXPECT_EQ(std::get<2>(emitted[0]), "a!");
    ASSERT_EQ(observed.requestStarts.size(), 3);
    EXPECT_GE(observed.requestStarts[1] - observed.requestStarts[0], std::chrono::milliseconds{150});
    EXPECT_GE(observed.requestStarts[2] - observed.requestStarts[1], std::chrono::milliseconds{150});
    ASSERT_EQ(observed.retries.size(), 2);
    EXPECT_TRUE(observed.retries[0].second);
    EXPECT_GE(observed.retries[0].first, std::chrono::milliseconds{150});
}

TEST_F(AsyncSourceTest, RetriesATransientlyFailedBatchAsABatch)
{
    startSource("3,0,1,0", 1, 0);
    observed.transientFailures = 1;
    handOff(handedOff(at(1, 1), {{"k", "a"}, {"k", "b"}, {"k", "c"}}));
    (*sink)->close();
    const auto emitted = drain();
    ASSERT_EQ(emitted.size(), 3);
    EXPECT_EQ(observed.requests, (std::vector<size_t>{3, 3})) << "not row by row, which would only load the service more";
}

TEST_F(AsyncSourceTest, FailsTheQueryWithTheFunctionsErrorOnceTransientRetriesAreUsedUp)
{
    startSource("1,0,1,0", 1, 0, 2);
    observed.transientFailures = 10;
    handOff(handedOff(at(1, 1), {{"k", "a"}}));
    (*sink)->close();
    try
    {
        drain();
        FAIL() << "the query should fail";
    }
    catch (const Exception& error)
    {
        EXPECT_NE(std::string_view{error.what()}.find("scripted transient failure"), std::string_view::npos) << error.what();
    }
    source->close();
    EXPECT_EQ(observed.requests.size(), 3) << "the first attempt and 2 retries";
}

TEST_F(AsyncSourceTest, FailsAtOnceWhenTheServiceAsksForAWaitBeyondTheTimeLimit)
{
    startSource("1,0,1,0", 1, 0, 8, 1);
    observed.transientFailures = 1;
    observed.retryAfter = std::chrono::seconds{5};
    handOff(handedOff(at(1, 1), {{"k", "a"}}));
    (*sink)->close();
    const auto start = std::chrono::steady_clock::now();
    EXPECT_THROW(drain(), Exception);
    source->close();
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds{2});
    EXPECT_EQ(observed.requests.size(), 1);
}

/// Two stages, e.g. the two steps of a pipeline, share a limit of 4 calls to one resource although each may run 8: the
/// limit holds across them, and a free permit goes to whichever stage has a call ready, so the small stage is not held
/// back until the big one is done.
TEST_F(AsyncSourceTest, StagesThatShareAResourceStayWithinItsLimitTogetherAndShareItFairly)
{
    const auto resource = fmt::format("shared-{}", channelId);
    const auto configuration = fmt::format("1,0,20,0,{},4", resource);
    startSource(configuration, 8, 0);
    const auto otherChannel = UUIDToString(generateUUID());
    auto other = makeSource(otherChannel, configuration, 8);
    HandoffChannel::Lease otherSink{otherChannel, 1024, HandoffChannel::Role::SINK};
    other->open(bufferManager);

    std::vector<std::pair<std::string, std::string>> big;
    for (int row = 0; row < 80; ++row)
    {
        big.emplace_back("k", fmt::format("big{}", row));
    }
    handOff(handedOff(at(1, 1), big));
    (*sink)->close();
    const auto start = std::chrono::steady_clock::now();
    std::chrono::steady_clock::duration bigTook{};
    std::chrono::steady_clock::duration smallTook{};
    std::jthread bigStage{[&]
                          {
                              EXPECT_EQ(drain().size(), 80);
                              bigTook = std::chrono::steady_clock::now() - start;
                          }};
    /// The small stage asks once the big one holds every permit.
    std::this_thread::sleep_for(std::chrono::milliseconds{30});
    handOff(otherSink, handedOff(at(1, 1), {{"k", "s1"}, {"k", "s2"}, {"k", "s3"}, {"k", "s4"}}));
    otherSink->close();
    EXPECT_EQ(drain(*other).size(), 4);
    smallTook = std::chrono::steady_clock::now() - start;
    bigStage.join();

    EXPECT_EQ(observed.mostInFlight, 4) << "the shared limit, not the 2 x 8 slots";
    EXPECT_LT(smallTook, bigTook / 2) << "the small stage waited for the big one to finish";
}

TEST(AsyncResourceLimiterTest, OneLimiterPerResourceWhileItIsHeld)
{
    const auto first = AsyncResourceLimiter::of("limiter-test-a", 3);
    const auto again = AsyncResourceLimiter::of("limiter-test-a", 5);
    EXPECT_EQ(first, again);
    EXPECT_EQ(again->limit(), 3) << "the first sets the limit";
    EXPECT_NE(AsyncResourceLimiter::of("limiter-test-b", 3), first);

    std::vector<AsyncResourceLimiter::Permit> permits;
    for (int permit = 0; permit < 3; ++permit)
    {
        auto taken = first->tryAcquire();
        ASSERT_TRUE(taken.has_value());
        permits.push_back(std::move(*taken));
    }
    EXPECT_FALSE(first->tryAcquire().has_value());
    permits.pop_back();
    EXPECT_TRUE(first->tryAcquire().has_value()) << "a returned permit is free again";
    EXPECT_EQ(first->mostInUse(), 3);
}

TEST_F(AsyncSourceTest, AStageThatStopsDoesNotWaitForAPermitOfTheSharedLimit)
{
    const auto configuration = fmt::format("1,0,600,0,stopping-{},1", channelId);
    startSource(configuration, 1, 0);
    const auto otherChannel = UUIDToString(generateUUID());
    auto other = makeSource(otherChannel, configuration, 1);
    HandoffChannel::Lease otherSink{otherChannel, 1024, HandoffChannel::Role::SINK};
    other->open(bufferManager);
    handOff(handedOff(at(1, 1), {{"k", "holds the permit"}}));
    (*sink)->close();
    std::jthread holder{[&] { EXPECT_EQ(drain().size(), 1); }};
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    handOff(otherSink, handedOff(at(1, 1), {{"k", "waits for it"}}));
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    const auto start = std::chrono::steady_clock::now();
    other->close();
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds{300}) << "the stop waited for the permit";
}

/// Input sorted by key, as SEM_SCAN's over a movie's reviews: 8 keys of 6 rows in buffers of 2. Every key in flight needs
/// its rows in the window before the next key's, so 8 slots stay busy only if the source reads ahead 48 rows, 24 buffers.
/// Bounding the read-ahead by 8 buffers, as before, kept 4 keys in flight and took twice as long; bounded by rows, all 8 run.
TEST_F(AsyncSourceTest, ReadsAheadPastRowsThatWaitForTheirKeyToKeepEveryKeyBusy)
{
    startSource("1,1,20,0", 8, 0);
    uint64_t sequence = 0;
    for (int key = 0; key < 8; ++key)
    {
        for (int row = 0; row < 6; row += 2)
        {
            handOff(handedOff(
                at(1, ++sequence),
                {{fmt::format("k{}", key), fmt::format("k{}-{}", key, row)},
                 {fmt::format("k{}", key), fmt::format("k{}-{}", key, row + 1)}}));
        }
    }
    (*sink)->close();
    const auto start = std::chrono::steady_clock::now();
    const auto emitted = drain();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    ASSERT_EQ(emitted.size(), 48);
    EXPECT_EQ(std::get<1>(emitted.back()), "k7-5") << "still emitted in stream order";
    EXPECT_EQ(observed.overlapsOfOneKey, 0);
    EXPECT_EQ(observed.mostInFlight, 8);
    /// 6 rounds of a row per key; 8 keys three at a time take 18.
    EXPECT_LT(elapsed, std::chrono::milliseconds{300});
}

TEST_F(AsyncSourceTest, ReadAheadStaysWithinItsBoundInRows)
{
    /// 12 rows held at most: two keys' rows, and the next key's first row as the front of the window moves on, so at most
    /// three keys in flight however many slots are free.
    startSource("1,1,10,0", 8, 0, AsyncSourceConfig{}.transientRetries, AsyncSourceConfig{}.transientRetrySeconds, 12);
    uint64_t sequence = 0;
    for (int key = 0; key < 4; ++key)
    {
        for (int row = 0; row < 6; ++row)
        {
            handOff(handedOff(at(1, ++sequence), {{fmt::format("k{}", key), fmt::format("k{}-{}", key, row)}}));
        }
    }
    (*sink)->close();
    EXPECT_EQ(drain().size(), 24);
    EXPECT_LE(observed.mostInFlight, 3);
}

TEST(KeyedRowQueueTest, StaysCheapWithManyRowsWaitingForTheirKeys)
{
    /// 256 keys of 64 rows, sorted by key, then every row run: quadratic work per take or finish would take seconds.
    KeyedRowQueue<int> queue;
    for (int key = 0; key < 256; ++key)
    {
        for (int row = 0; row < 64; ++row)
        {
            queue.push(key * 64 + row, {std::to_string(key)});
        }
    }
    EXPECT_EQ(queue.startableRows(), 256);
    const auto start = std::chrono::steady_clock::now();
    size_t ran = 0;
    while (queue.size() > 0)
    {
        for (auto& [row, keys] : queue.take(256))
        {
            queue.finish(keys);
            ++ran;
        }
    }
    EXPECT_EQ(ran, 256 * 64);
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds{500});
}

}
}
