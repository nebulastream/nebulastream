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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/ReplayableSourceStorage.hpp>
#include <Sources/SourceCheckpointProgress.hpp>
#include <Sources/SourceCheckpointing.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/UUID.hpp>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <BaseUnitTest.hpp>
#include <QueryId.hpp>
#include <SourceThread.hpp>
#include <TestSource.hpp>

namespace NES
{
namespace
{
constexpr std::chrono::seconds Timeout{5};
constexpr std::chrono::hours NoScheduledCheckpoints{1};

struct EmittedBuffer
{
    SequenceNumber sequenceNumber;
    std::vector<std::byte> payload;

    bool operator==(const EmittedBuffer&) const = default;
};

std::vector<std::byte> payloadOf(const uint8_t value)
{
    return std::vector<std::byte>(16, std::byte{value});
}

class EmittedBuffers
{
public:
    SourceReturnType::EmitFunction recorder()
    {
        return [this](const OriginId, SourceReturnType::SourceReturnType event, const std::stop_token&)
        {
            if (const auto* data = std::get_if<SourceReturnType::Data>(&event))
            {
                const auto payload = data->buffer.getAvailableMemoryArea().first(data->buffer.getNumberOfTuples());
                const std::scoped_lock lock(mutex);
                buffers.push_back({data->buffer.getSequenceNumber(), std::vector(payload.begin(), payload.end())});
                emitted.notify_all();
            }
            return SourceReturnType::EmitResult::SUCCESS;
        };
    }

    std::vector<EmittedBuffer> waitFor(const size_t numberOfBuffers)
    {
        std::unique_lock lock(mutex);
        EXPECT_TRUE(emitted.wait_for(lock, Timeout, [&] { return buffers.size() >= numberOfBuffers; }))
            << "Timeout waiting for " << numberOfBuffers << " buffers";
        return buffers;
    }

private:
    std::mutex mutex;
    std::condition_variable emitted;
    std::vector<EmittedBuffer> buffers;
};
}

class SourceCheckpointingTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SourceCheckpointingTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager
            = BufferManager::create(64 * 1024 * 1024, 0.5, BufferAlignment{64}, 4096, std::make_shared<NesDefaultMemoryAllocator>());
        checkpointDirectory = std::filesystem::temp_directory_path()
            / ("source-checkpointing-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    }

    void TearDown() override
    {
        std::error_code errorCode;
        std::filesystem::remove_all(checkpointDirectory, errorCode);
        BaseUnitTest::TearDown();
    }

    [[nodiscard]] TupleBuffer bufferWith(const std::vector<std::byte>& payload) const
    {
        auto buffer = bufferManager->getBufferBlocking();
        std::ranges::copy(payload, buffer.getAvailableMemoryArea().begin());
        buffer.setNumberOfTuples(payload.size());
        return buffer;
    }

    std::shared_ptr<BufferManager> bufferManager;
    std::filesystem::path checkpointDirectory;
};

TEST_F(SourceCheckpointingTest, ProgressOnlyAdvancesOverContiguouslyCompletedSequences)
{
    SourceCheckpointProgress progress;
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), INVALID_SEQ_NUMBER.getRawValue());
    progress.noteCompletedSequence(SequenceNumber(2));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), INVALID_SEQ_NUMBER.getRawValue());
    progress.noteCompletedSequence(SequenceNumber(1));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), 2U);
    progress.noteCompletedSequence(SequenceNumber(4));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), 2U);
    progress.noteCompletedSequence(SequenceNumber(3));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), 4U);
}

TEST_F(SourceCheckpointingTest, StorageReplaysBuffersAfterTheCheckpointedSequence)
{
    {
        ReplayableSourceStorage storage(checkpointDirectory);
        storage.persist(1, bufferWith(payloadOf(1)));
        storage.persist(2, bufferWith(payloadOf(2)));
        storage.persist(3, bufferWith(payloadOf(3)));
        storage.prune(1);
    }

    const ReplayableSourceStorage storage(checkpointDirectory);
    EXPECT_EQ(storage.getByteOffsetAfter(2), 2 * payloadOf(0).size());
    EXPECT_EQ(storage.getByteOffsetAfter(std::numeric_limits<SequenceNumber::Underlying>::max()), 3 * payloadOf(0).size());

    std::vector<EmittedBuffer> replayed;
    const auto numberOfReplayedBuffers = storage.replayAfter(
        2,
        *bufferManager,
        [&](const SequenceNumber::Underlying sequence, TupleBuffer&& buffer)
        {
            const auto payload = buffer.getAvailableMemoryArea().first(buffer.getNumberOfTuples());
            replayed.push_back({SequenceNumber(sequence), std::vector(payload.begin(), payload.end())});
        },
        std::stop_token{});
    EXPECT_EQ(numberOfReplayedBuffers, 1U);
    EXPECT_EQ(replayed, (std::vector<EmittedBuffer>{{SequenceNumber(3), payloadOf(3)}}));
    EXPECT_ANY_THROW(std::ignore = storage.getByteOffsetAfter(0));
}

TEST_F(SourceCheckpointingTest, RecoveredSourceReplaysUncheckpointedBuffersBeforeNewInput)
{
    const auto queryId = QueryId::createLocal(LocalQueryId(generateUUID()));
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    {
        const auto checkpointManager = std::make_shared<CheckpointManager>(checkpointDirectory, NoScheduledCheckpoints, false);
        auto control = std::make_shared<TestSourceControl>();
        for (const uint8_t value : {1, 2, 3})
        {
            control->injectData(payloadOf(value), 1);
        }
        SourceThread sourceThread(
            backpressureListener,
            INITIAL<OriginId>,
            bufferManager,
            std::make_unique<TestSource>(INITIAL<OriginId>, control),
            SourceCheckpointing{.checkpointManager = checkpointManager, .queryId = queryId});
        EmittedBuffers emitted;
        ASSERT_TRUE(sourceThread.start(emitted.recorder()));
        emitted.waitFor(3);
        sourceThread.getCheckpointProgress()->noteCompletedSequence(SequenceNumber(1));
        checkpointManager->runCheckpointRound();
        sourceThread.stop();
    }

    const auto recoveringCheckpointManager = std::make_shared<CheckpointManager>(checkpointDirectory, NoScheduledCheckpoints, true);
    auto control = std::make_shared<TestSourceControl>();
    control->injectData(payloadOf(4), 1);
    SourceThread recoveredSourceThread(
        backpressureListener,
        INITIAL<OriginId>,
        bufferManager,
        std::make_unique<TestSource>(INITIAL<OriginId>, control),
        SourceCheckpointing{.checkpointManager = recoveringCheckpointManager, .queryId = queryId});
    EmittedBuffers emitted;
    ASSERT_TRUE(recoveredSourceThread.start(emitted.recorder()));
    EXPECT_EQ(
        emitted.waitFor(3),
        (std::vector<EmittedBuffer>{
            {SequenceNumber(1), payloadOf(2)}, {SequenceNumber(2), payloadOf(3)}, {SequenceNumber(3), payloadOf(4)}}));
    recoveredSourceThread.stop();
}

}
