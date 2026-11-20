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
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>
#include <librdkafka/rdkafkacpp.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <KafkaSource.hpp>

namespace NES
{

namespace
{
constexpr uint32_t BUFFER_SIZE = 4096;
constexpr uint32_t NUM_BUFFERS = 16;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUM_BUFFERS) * BUFFER_SIZE;
constexpr std::string_view TOPIC = "sensors";
constexpr int32_t DEFAULT_PARTITION_COUNT = 3;

/// An in-process librdkafka mock cluster with one topic, plus a producer to fill it. No network, no docker.
class MockKafka
{
public:
    explicit MockKafka(const int32_t partitionCount = 1) : partitionCount(partitionCount)
    {
        std::string error;
        const std::unique_ptr<RdKafka::Conf> conf{RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)};
        conf->set("test.mock.num.brokers", "1", error);
        producer.reset(RdKafka::Producer::create(conf.get(), error));
        EXPECT_NE(producer, nullptr) << error;
        cluster = rd_kafka_handle_mock_cluster(producer->c_ptr());
        rd_kafka_mock_topic_create(cluster, std::string{TOPIC}.c_str(), partitionCount, 1);
        bootstrap = rd_kafka_mock_cluster_bootstraps(cluster);
    }

    void produce(const int32_t partition, const std::string& payload)
    {
        const auto err = producer->produce(
            std::string{TOPIC},
            partition,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(payload.data()), ///NOLINT(cppcoreguidelines-pro-type-const-cast) librdkafka copies it
            payload.size(),
            nullptr,
            0,
            0,
            nullptr);
        ASSERT_EQ(err, RdKafka::ERR_NO_ERROR) << RdKafka::err2str(err);
    }

    /// A tombstone: key, no payload.
    void produceTombstone(const int32_t partition, const std::string& key)
    {
        const auto err = producer->produce(
            std::string{TOPIC}, partition, RdKafka::Producer::RK_MSG_COPY, nullptr, 0, key.data(), key.size(), 0, nullptr);
        ASSERT_EQ(err, RdKafka::ERR_NO_ERROR) << RdKafka::err2str(err);
    }

    void flush(const int timeoutMs = 5000) { ASSERT_EQ(producer->flush(timeoutMs), RdKafka::ERR_NO_ERROR); }

    /// Committed offsets of `group` per partition, read with a separate consumer.
    std::vector<int64_t> committed(const std::string& group)
    {
        std::string error;
        const std::unique_ptr<RdKafka::Conf> conf{RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)};
        conf->set("bootstrap.servers", bootstrap, error);
        conf->set("group.id", group, error);
        const std::unique_ptr<RdKafka::KafkaConsumer> reader{RdKafka::KafkaConsumer::create(conf.get(), error)};
        std::vector<RdKafka::TopicPartition*> partitions;
        partitions.reserve(partitionCount);
        for (int32_t p = 0; p < partitionCount; ++p)
        {
            partitions.push_back(RdKafka::TopicPartition::create(std::string{TOPIC}, p));
        }
        EXPECT_EQ(reader->committed(partitions, 5000), RdKafka::ERR_NO_ERROR);
        std::vector<int64_t> offsets;
        offsets.reserve(partitions.size());
        for (auto* partition : partitions)
        {
            offsets.push_back(partition->offset());
        }
        RdKafka::TopicPartition::destroy(partitions);
        reader->close();
        return offsets;
    }

    /// The next `count` OffsetCommit requests fail with a non-retriable error.
    void failNextCommits(const size_t count)
    {
        constexpr int16_t OFFSET_COMMIT_API_KEY = 8;
        for (size_t i = 0; i < count; ++i)
        {
            /// librdkafka's mock API is inherently variadic
            ///NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
            rd_kafka_mock_push_request_errors(cluster, OFFSET_COMMIT_API_KEY, 1, RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED);
        }
    }

    /// The next OffsetFetch request fails with "not coordinator", as a freshly started broker answers while it creates the offsets topic.
    void failNextOffsetFetch()
    {
        constexpr int16_t OFFSET_FETCH_API_KEY = 9;
        /// librdkafka's mock API is inherently variadic
        ///NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
        rd_kafka_mock_push_request_errors(cluster, OFFSET_FETCH_API_KEY, 1, RD_KAFKA_RESP_ERR_NOT_COORDINATOR);
    }

    void setBrokerDown(const bool down)
    {
        if (down)
        {
            rd_kafka_mock_broker_set_down(cluster, 1);
        }
        else
        {
            rd_kafka_mock_broker_set_up(cluster, 1);
        }
    }

    std::string bootstrap;

private:
    int32_t partitionCount;
    std::unique_ptr<RdKafka::Producer> producer;
    rd_kafka_mock_cluster_t* cluster = nullptr;
};
}

class KafkaSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("KafkaSourceTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    }

    static KafkaSourceConfig config(const MockKafka& kafka, const std::string& group)
    {
        return KafkaSourceConfig{
            .brokers = kafka.bootstrap,
            .topic = std::string{TOPIC},
            .groupId = group,
            .autoOffsetReset = "earliest",
            .synchronousCommit = true,
            .flushIntervalMs = 200,
            .implicitMessageDelimiter = "\n"};
    }

    /// Fills buffers until `expectedRecords` newline-terminated records were read or `timeout` passed.
    /// An idle fill blocks until data arrives, so the deadline is enforced the way the engine stops a source:
    /// by requesting a stop, after which the fill returns.
    std::vector<std::string>
    readRecords(KafkaSource& source, const size_t expectedRecords, const std::chrono::milliseconds timeout = std::chrono::seconds(20))
    {
        std::vector<std::string> records;
        std::string partial;
        std::stop_source stop;
        const std::jthread timer{[&stop, timeout](const std::stop_token& cancelled)
                                 {
                                     const auto deadline = std::chrono::steady_clock::now() + timeout;
                                     while (!cancelled.stop_requested() && std::chrono::steady_clock::now() < deadline)
                                     {
                                         std::this_thread::sleep_for(std::chrono::milliseconds(10));
                                     }
                                     stop.request_stop();
                                 }};
        while (records.size() < expectedRecords && !stop.stop_requested())
        {
            auto buffer = bufferManager->getBufferBlocking();
            const auto result = source.fillTupleBuffer(buffer, stop.get_token());
            if (result.isEoS())
            {
                break;
            }
            partial.append(std::bit_cast<const char*>(buffer.getAvailableMemoryArea().data()), result.getNumberOfBytes());
            size_t newline = 0;
            while ((newline = partial.find('\n')) != std::string::npos)
            {
                records.push_back(partial.substr(0, newline));
                partial.erase(0, newline + 1);
            }
        }
        return records;
    }

    /// Fills buffers until at least `expectedBytes` were read or `timeout` passed, and returns everything read.
    std::string
    readBytes(KafkaSource& source, const size_t expectedBytes, const std::chrono::milliseconds timeout = std::chrono::seconds(20))
    {
        std::string bytes;
        std::stop_source stop;
        const std::jthread timer{[&stop, timeout](const std::stop_token& cancelled)
                                 {
                                     const auto deadline = std::chrono::steady_clock::now() + timeout;
                                     while (!cancelled.stop_requested() && std::chrono::steady_clock::now() < deadline)
                                     {
                                         std::this_thread::sleep_for(std::chrono::milliseconds(10));
                                     }
                                     stop.request_stop();
                                 }};
        while (bytes.size() < expectedBytes && !stop.stop_requested())
        {
            auto buffer = bufferManager->getBufferBlocking();
            const auto result = source.fillTupleBuffer(buffer, stop.get_token());
            if (result.isEoS())
            {
                break;
            }
            bytes.append(std::bit_cast<const char*>(buffer.getAvailableMemoryArea().data()), result.getNumberOfBytes());
        }
        return bytes;
    }

    std::string fillOnce(KafkaSource& source)
    {
        const std::stop_source stop;
        auto buffer = bufferManager->getBufferBlocking();
        const auto result = source.fillTupleBuffer(buffer, stop.get_token());
        return std::string{std::bit_cast<const char*>(buffer.getAvailableMemoryArea().data()), result.getNumberOfBytes()};
    }

    std::shared_ptr<BufferManager> bufferManager;
};

TEST_F(KafkaSourceTest, ConsumesAllPartitionsAndCommitsAfterTheNextFill)
{
    MockKafka kafka{DEFAULT_PARTITION_COUNT};
    for (int i = 0; i < 300; ++i)
    {
        kafka.produce(i % DEFAULT_PARTITION_COUNT, std::to_string(i));
    }
    kafka.flush();

    KafkaSource source{config(kafka, "g-all")};
    source.open(bufferManager);
    auto records = readRecords(source, 300);
    std::ranges::sort(records, {}, [](const auto& r) { return std::stoi(r); });
    ASSERT_EQ(records.size(), 300);
    for (int i = 0; i < 300; ++i)
    {
        EXPECT_EQ(records[i], std::to_string(i));
    }

    source.close();
    EXPECT_EQ(kafka.committed("g-all"), (std::vector<int64_t>{100, 100, 100}));
    EXPECT_EQ(source.getFailedCommits(), 0);
}

TEST_F(KafkaSourceTest, FailedCommitsAreCountedInBothCommitModes)
{
    for (const bool synchronous : {true, false})
    {
        SCOPED_TRACE(synchronous ? "SYNC" : "ASYNC");
        MockKafka kafka;
        kafka.produce(0, "a");
        kafka.flush();
        auto sourceConfig = config(kafka, synchronous ? "g-fail-sync" : "g-fail-async");
        sourceConfig.synchronousCommit = synchronous;
        KafkaSource source{sourceConfig};
        source.open(bufferManager);
        ASSERT_EQ(readRecords(source, 1), std::vector<std::string>{"a"});
        kafka.failNextCommits(1);
        kafka.produce(0, "b");
        kafka.flush();
        /// The fill after "a"'s buffer commits it (and fails); an async result is reported on a later poll.
        ASSERT_EQ(readRecords(source, 1), std::vector<std::string>{"b"});
        std::ignore = readRecords(source, 1, std::chrono::milliseconds(1500));
        EXPECT_GE(source.getFailedCommits(), 1);
        source.close();
    }
}

TEST_F(KafkaSourceTest, RestartWithSameGroupResumesAfterCommittedOffsets)
{
    MockKafka kafka;
    for (int i = 0; i < 30; ++i)
    {
        kafka.produce(0, std::to_string(i));
    }
    kafka.flush();
    const auto sourceConfig = config(kafka, "g-restart");
    {
        KafkaSource first{sourceConfig};
        first.open(bufferManager);
        ASSERT_EQ(readRecords(first, 30).size(), 30);
        first.close();
    }
    for (int i = 30; i < 40; ++i)
    {
        kafka.produce(0, std::to_string(i));
    }
    kafka.flush();

    KafkaSource second{sourceConfig};
    second.open(bufferManager);
    const auto records = readRecords(second, 10);
    second.close();
    ASSERT_EQ(records.size(), 10);
    EXPECT_EQ(records.front(), "30");
    EXPECT_EQ(records.back(), "39");
}

TEST_F(KafkaSourceTest, LatestSkipsMessagesProducedBeforeOpen)
{
    MockKafka kafka;
    kafka.produce(0, "old");
    kafka.flush();
    auto sourceConfig = config(kafka, "g-latest");
    sourceConfig.autoOffsetReset = "latest";
    KafkaSource source{sourceConfig};
    source.open(bufferManager);
    /// Let the assignment settle on the end offset before producing.
    EXPECT_TRUE(readRecords(source, 1, std::chrono::seconds(2)).empty());
    kafka.produce(0, "new");
    kafka.flush();
    EXPECT_EQ(readRecords(source, 1), std::vector<std::string>{"new"});
    source.close();
}

TEST_F(KafkaSourceTest, EveryMessageIsFollowedByTheImplicitDelimiter)
{
    MockKafka kafka;
    kafka.produce(0, "a");
    kafka.produce(0, "b");
    kafka.flush();
    auto sourceConfig = config(kafka, "g-delimiter");
    sourceConfig.implicitMessageDelimiter = ";";
    KafkaSource source{sourceConfig};
    source.open(bufferManager);
    EXPECT_EQ(readBytes(source, 4), "a;b;");
    source.close();
}

TEST_F(KafkaSourceTest, EmptyDelimiterAppendsNothing)
{
    MockKafka kafka;
    kafka.produce(0, "a\n");
    kafka.produce(0, "b\n");
    kafka.flush();
    auto sourceConfig = config(kafka, "g-no-delimiter");
    sourceConfig.implicitMessageDelimiter = "";
    KafkaSource source{sourceConfig};
    source.open(bufferManager);
    EXPECT_EQ(readBytes(source, 4), "a\nb\n");
    source.close();
}

TEST_F(KafkaSourceTest, FlushIntervalHandsOverAPartlyFilledBuffer)
{
    MockKafka kafka;
    kafka.produce(0, "a");
    kafka.produce(0, "b");
    kafka.produce(0, "c");
    kafka.flush();
    KafkaSource source{config(kafka, "g-flush")};
    source.open(bufferManager);
    const auto filled = fillOnce(source);
    EXPECT_FALSE(filled.empty());
    EXPECT_LT(filled.size(), BUFFER_SIZE);
    source.close();
}

TEST_F(KafkaSourceTest, OversizedMessageArrivesIntactAndCommitsOnlyOnceComplete)
{
    MockKafka kafka;
    std::string big(3 * BUFFER_SIZE, 'x');
    big.back() = 'y';
    kafka.produce(0, big);
    kafka.flush();
    KafkaSource source{config(kafka, "g-big")};
    source.open(bufferManager);

    const std::stop_source stop;
    std::string received;
    /// Three full buffers + one with the tail and the delimiter.
    for (int i = 0; i < 3; ++i)
    {
        auto buffer = bufferManager->getBufferBlocking();
        const auto result = source.fillTupleBuffer(buffer, stop.get_token());
        ASSERT_EQ(result.getNumberOfBytes(), BUFFER_SIZE);
        received.append(std::bit_cast<const char*>(buffer.getAvailableMemoryArea().data()), result.getNumberOfBytes());
        /// Nothing is committed while part of the message is still in the stash.
        EXPECT_EQ(kafka.committed("g-big")[0], RdKafka::Topic::OFFSET_INVALID);
    }
    const auto rest = readRecords(source, 1);
    ASSERT_EQ(rest.size(), 1);
    received.append(rest[0]);
    EXPECT_EQ(received, big);
    source.close();
    EXPECT_EQ(kafka.committed("g-big")[0], 1);
}

TEST_F(KafkaSourceTest, DelimiterThatDoesNotFitMovesToTheNextBufferAlone)
{
    MockKafka kafka;
    kafka.produce(0, std::string(BUFFER_SIZE, 'x'));
    kafka.produce(0, std::string(BUFFER_SIZE - 1, 'y'));
    kafka.flush();
    KafkaSource source{config(kafka, "g-delim")};
    source.open(bufferManager);

    /// The payload fills the buffer, so its delimiter alone starts the next one. The second record's payload fills the rest
    /// of that buffer; its delimiter is still in the stash, so only the first message is committed.
    EXPECT_EQ(fillOnce(source), std::string(BUFFER_SIZE, 'x'));
    EXPECT_EQ(kafka.committed("g-delim")[0], RdKafka::Topic::OFFSET_INVALID);
    EXPECT_EQ(fillOnce(source), "\n" + std::string(BUFFER_SIZE - 1, 'y'));
    source.close();
    EXPECT_EQ(kafka.committed("g-delim")[0], 1);
}

TEST_F(KafkaSourceTest, TombstonesAreSkippedAndCounted)
{
    MockKafka kafka;
    kafka.produce(0, "a");
    kafka.produceTombstone(0, "deleted-key");
    kafka.produce(0, "b");
    kafka.flush();
    KafkaSource source{config(kafka, "g-tombstone")};
    source.open(bufferManager);
    EXPECT_EQ(readRecords(source, 2), (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(source.getSkippedMessages(), 1);
    source.close();
}

TEST_F(KafkaSourceTest, UnreachableBrokerFailsOpenWithinTimeout)
{
    KafkaSource source{KafkaSourceConfig{
        .brokers = "127.0.0.1:1",
        .topic = "t",
        .groupId = "g",
        .autoOffsetReset = "latest",
        .synchronousCommit = true,
        .flushIntervalMs = 0,
        .implicitMessageDelimiter = "\n"}};
    const auto start = std::chrono::steady_clock::now();
    EXPECT_ANY_THROW(source.open(bufferManager));
    EXPECT_LT(std::chrono::steady_clock::now() - start, KafkaSource::OPEN_TIMEOUT + std::chrono::seconds(2));
    source.close();
}

TEST_F(KafkaSourceTest, MissingTopicFailsOpen)
{
    MockKafka kafka;
    auto sourceConfig = config(kafka, "g-missing");
    sourceConfig.topic = "missing";
    KafkaSource source{sourceConfig};
    EXPECT_ANY_THROW(source.open(bufferManager));
    source.close();
}

TEST_F(KafkaSourceTest, ConnectionErrorsThatRetryingCannotFixAreFatal)
{
    for (const auto code : {RdKafka::ERR__SSL, RdKafka::ERR__AUTHENTICATION, RdKafka::ERR__FATAL})
    {
        EXPECT_TRUE(KafkaSource::isFatalConnectionError(code)) << RdKafka::err2str(code);
        EXPECT_TRUE(KafkaSource::isFatalError(code)) << RdKafka::err2str(code);
    }
    for (const auto code : {RdKafka::ERR__TRANSPORT, RdKafka::ERR__ALL_BROKERS_DOWN, RdKafka::ERR__TIMED_OUT, RdKafka::ERR_NO_ERROR})
    {
        EXPECT_FALSE(KafkaSource::isFatalConnectionError(code)) << RdKafka::err2str(code);
        EXPECT_FALSE(KafkaSource::isFatalError(code)) << RdKafka::err2str(code);
    }
    for (const auto code :
         {RdKafka::ERR__UNKNOWN_TOPIC,
          RdKafka::ERR__UNKNOWN_PARTITION,
          RdKafka::ERR_UNKNOWN_TOPIC_OR_PART,
          RdKafka::ERR_TOPIC_AUTHORIZATION_FAILED,
          RdKafka::ERR_GROUP_AUTHORIZATION_FAILED,
          RdKafka::ERR__INVALID_ARG})
    {
        EXPECT_TRUE(KafkaSource::isFatalError(code)) << RdKafka::err2str(code);
        EXPECT_FALSE(KafkaSource::isFatalConnectionError(code)) << RdKafka::err2str(code);
    }
}

TEST_F(KafkaSourceTest, StopRequestEndsAnIdleFillPromptly)
{
    MockKafka kafka;
    auto sourceConfig = config(kafka, "g-stop");
    sourceConfig.flushIntervalMs = 0;
    KafkaSource source{sourceConfig};
    source.open(bufferManager);
    std::stop_source stop;
    auto buffer = bufferManager->getBufferBlocking();
    const std::jthread stopper{[&stop]
                               {
                                   std::this_thread::sleep_for(std::chrono::milliseconds(300));
                                   stop.request_stop();
                               }};
    const auto start = std::chrono::steady_clock::now();
    /// Nothing read: EoS rather than an empty buffer, which SourceThread would emit. With a stop requested, SourceThread maps EoS to
    /// StopRequested.
    const auto result = source.fillTupleBuffer(buffer, stop.get_token());
    EXPECT_TRUE(result.isEoS());
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(300) + (3 * KafkaSource::POLL_TIMEOUT));
    source.close();
}

TEST_F(KafkaSourceTest, DataReturnedWithAStopRequestIsNeverCommitted)
{
    /// On a stop request SourceThread may drop the returned buffer instead of emitting it: its offsets must stay
    /// uncommitted so a restart reads the data again.
    MockKafka kafka;
    kafka.produce(0, "in-flight");
    kafka.flush();
    auto sourceConfig = config(kafka, "g-stop-commit");
    sourceConfig.flushIntervalMs = 0;
    KafkaSource source{sourceConfig};
    source.open(bufferManager);
    auto buffer = bufferManager->getBufferBlocking();
    auto result = Source::FillTupleBufferResult::eos();
    /// EoS means the stop arrived before the consumer delivered the record (the first fetch takes as long as the machine is loaded):
    /// nothing was consumed, so fill again until the record came back together with a stop request.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (result.isEoS() && std::chrono::steady_clock::now() < deadline)
    {
        std::stop_source stop;
        const std::jthread stopper{[&stop]
                                   {
                                       std::this_thread::sleep_for(std::chrono::milliseconds(100));
                                       stop.request_stop();
                                   }};
        result = source.fillTupleBuffer(buffer, stop.get_token());
    }
    ASSERT_FALSE(result.isEoS());
    EXPECT_GT(result.getNumberOfBytes(), 0);
    source.close();
    EXPECT_EQ(kafka.committed("g-stop-commit")[0], RdKafka::Topic::OFFSET_INVALID);
}

TEST_F(KafkaSourceTest, BrokerOutageKeepsTheSourceRunningAndResumes)
{
    MockKafka kafka;
    kafka.produce(0, "before");
    kafka.flush();
    KafkaSource source{config(kafka, "g-outage")};
    source.open(bufferManager);
    EXPECT_EQ(readRecords(source, 1), std::vector<std::string>{"before"});

    kafka.setBrokerDown(true);
    EXPECT_NO_THROW(EXPECT_TRUE(readRecords(source, 1, std::chrono::seconds(3)).empty()));
    kafka.setBrokerDown(false);
    kafka.produce(0, "after");
    /// Producer and consumer reconnect with librdkafka's backoff (up to 10 s by default).
    kafka.flush(30000);
    EXPECT_EQ(readRecords(source, 1, std::chrono::seconds(40)), std::vector<std::string>{"after"});
    source.close();
}

TEST_F(KafkaSourceTest, ANotCoordinatorAnswerToTheFirstOffsetLookupDoesNotLeaveTheSourceIdle)
{
    MockKafka kafka;
    kafka.produce(0, "first");
    kafka.flush();
    kafka.failNextOffsetFetch();
    KafkaSource source{config(kafka, "g-not-coordinator")};
    source.open(bufferManager);
    EXPECT_EQ(readRecords(source, 1, std::chrono::seconds(20)), std::vector<std::string>{"first"});
    source.close();
}

TEST_F(KafkaSourceTest, AsyncCommitsDoNotBlockAFillDuringAnOutage)
{
    MockKafka kafka;
    kafka.produce(0, "x");
    kafka.flush();
    auto sourceConfig = config(kafka, "g-async-outage");
    sourceConfig.synchronousCommit = false;
    KafkaSource source{sourceConfig};
    source.open(bufferManager);
    ASSERT_EQ(readRecords(source, 1), std::vector<std::string>{"x"});
    kafka.setBrokerDown(true);
    /// The next fill commits "x"'s buffer; with ASYNC it must not wait for the unreachable broker.
    const auto start = std::chrono::steady_clock::now();
    std::ignore = readRecords(source, 1, std::chrono::milliseconds(500));
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
    kafka.setBrokerDown(false);
    source.close();
}

TEST_F(KafkaSourceTest, CloseDuringABrokerOutageReturnsWithinTheCommitTimeout)
{
    MockKafka kafka;
    kafka.produce(0, "x");
    kafka.flush();
    KafkaSource source{config(kafka, "g-close-outage")};
    source.open(bufferManager);
    ASSERT_EQ(readRecords(source, 1), std::vector<std::string>{"x"});
    kafka.setBrokerDown(true);
    /// The final commit cannot reach a broker: close() must give up instead of blocking the query's stop.
    const auto start = std::chrono::steady_clock::now();
    source.close();
    /// At most the final commit's timeout plus the consumer close's timeout.
    EXPECT_LT(std::chrono::steady_clock::now() - start, (2 * KafkaSource::COMMIT_TIMEOUT) + std::chrono::seconds(2));
    EXPECT_EQ(source.getFailedCommits(), 1);
}

}
