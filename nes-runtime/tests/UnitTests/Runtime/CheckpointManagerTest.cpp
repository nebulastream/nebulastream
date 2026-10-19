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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Util/UUID.hpp>
#include <QueryId.hpp>

namespace NES
{

class CheckpointManagerTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        checkpointDirectory = std::filesystem::temp_directory_path()
            / ("checkpoint-manager-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(checkpointDirectory);
    }

    void TearDown() override
    {
        std::error_code errorCode;
        std::filesystem::remove_all(checkpointDirectory, errorCode);
    }

    void writeFile(const std::filesystem::path& path, const std::string& contents) const
    {
        std::filesystem::create_directories(path.parent_path());
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.is_open()) << path;
        out << contents;
    }

    std::filesystem::path checkpointDirectory;
};

TEST_F(CheckpointManagerTest, RunsPrepareCallbacksBeforeCommitCallbacksInStableOrder)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    std::vector<std::string> executionOrder;

    manager.registerCallback(
        "prepare_b", [&](uint64_t) { executionOrder.emplace_back("prepare_b"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback(
        "commit_z", [&](uint64_t) { executionOrder.emplace_back("commit_z"); }, CheckpointManager::CallbackPhase::Commit);
    manager.registerCallback(
        "prepare_a", [&](uint64_t) { executionOrder.emplace_back("prepare_a"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback(
        "commit_a", [&](uint64_t) { executionOrder.emplace_back("commit_a"); }, CheckpointManager::CallbackPhase::Commit);

    manager.runCheckpointRound();

    EXPECT_EQ(executionOrder, (std::vector<std::string>{"prepare_a", "prepare_b", "commit_a", "commit_z"}));
}

TEST_F(CheckpointManagerTest, ReRegisteringCallbackMovesItBetweenPhases)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    std::vector<std::string> executionOrder;

    manager.registerCallback(
        "shared", [&](uint64_t) { executionOrder.emplace_back("prepare"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback("shared", [&](uint64_t) { executionOrder.emplace_back("commit"); }, CheckpointManager::CallbackPhase::Commit);

    manager.runCheckpointRound();

    EXPECT_EQ(executionOrder, (std::vector<std::string>{"commit"}));
}

TEST_F(CheckpointManagerTest, FailingPrepareCallbackSkipsCommitPhase)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    bool commitRan = false;

    manager.registerCallback(
        "prepare", [](uint64_t) { throw std::runtime_error("cannot persist state"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback("commit", [&](uint64_t) { commitRan = true; }, CheckpointManager::CallbackPhase::Commit);

    EXPECT_THROW(manager.runCheckpointRound(), std::runtime_error);
    EXPECT_FALSE(commitRan);
}

TEST_F(CheckpointManagerTest, PrepareGuardIsHeldWhileItsPrefixedCallbacksRun)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    std::vector<std::string> executionOrder;

    manager.registerPrepareGuard(
        "query/1/",
        [&](uint64_t)
        {
            executionOrder.emplace_back("acquire");
            return std::shared_ptr<void>(nullptr, [&](void*) { executionOrder.emplace_back("release"); });
        });
    manager.registerCallback(
        "query/1/source", [&](uint64_t) { executionOrder.emplace_back("source"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback(
        "query/1/state", [&](uint64_t) { executionOrder.emplace_back("state"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback(
        "query/2/state", [&](uint64_t) { executionOrder.emplace_back("other"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback(
        "query/1/commit", [&](uint64_t) { executionOrder.emplace_back("commit"); }, CheckpointManager::CallbackPhase::Commit);

    manager.runCheckpointRound();

    EXPECT_EQ(executionOrder, (std::vector<std::string>{"acquire", "source", "state", "release", "other", "commit"}));
}

TEST_F(CheckpointManagerTest, FailingPrepareGuardSkipsRound)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    bool callbackRan = false;

    manager.registerPrepareGuard("query/1/", [](uint64_t) -> std::shared_ptr<void> { throw std::runtime_error("query is busy"); });
    manager.registerCallback("query/1/state", [&](uint64_t) { callbackRan = true; }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback("query/1/commit", [&](uint64_t) { callbackRan = true; }, CheckpointManager::CallbackPhase::Commit);

    EXPECT_THROW(manager.runCheckpointRound(), std::runtime_error);
    EXPECT_FALSE(callbackRan);
}

TEST_F(CheckpointManagerTest, GuardCanLeaveItsCallbacksOutOfTheRound)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    std::vector<std::string> executionOrder;

    manager.registerPrepareGuard("query/1/", [](uint64_t) -> std::optional<std::shared_ptr<void>> { return std::nullopt; });
    manager.registerCallback(
        "query/1/state", [&](uint64_t) { executionOrder.emplace_back("skipped"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback(
        "query/2/state", [&](uint64_t) { executionOrder.emplace_back("other"); }, CheckpointManager::CallbackPhase::Prepare);

    manager.runCheckpointRound();

    EXPECT_EQ(executionOrder, (std::vector<std::string>{"other"}));
    EXPECT_EQ(CheckpointManager::loadFile(checkpointDirectory / "completed_round"), "1");
}

TEST_F(CheckpointManagerTest, RoundIncludesCallbacksRegisteredUntilTheirGuardIsAcquired)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    std::vector<std::string> executionOrder;

    manager.registerPrepareGuard(
        "query/1/",
        [&](uint64_t)
        {
            manager.registerCallback(
                "query/1/source", [&](uint64_t) { executionOrder.emplace_back("source"); }, CheckpointManager::CallbackPhase::Prepare);
            return std::shared_ptr<void>();
        });
    manager.registerCallback(
        "query/1/state", [&](uint64_t) { executionOrder.emplace_back("state"); }, CheckpointManager::CallbackPhase::Prepare);

    manager.runCheckpointRound();

    EXPECT_EQ(executionOrder, (std::vector<std::string>{"source", "state"}));
}

TEST_F(CheckpointManagerTest, UnregisterCallbacksWithPrefixKeepsOtherCallbacks)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    std::vector<std::string> executionOrder;

    manager.registerCallback(
        "query/1/state", [&](uint64_t) { executionOrder.emplace_back("state"); }, CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback(
        "query/1/commit", [&](uint64_t) { executionOrder.emplace_back("commit"); }, CheckpointManager::CallbackPhase::Commit);
    manager.registerCallback(
        "query/10/state", [&](uint64_t) { executionOrder.emplace_back("other"); }, CheckpointManager::CallbackPhase::Prepare);

    manager.unregisterCallbacksWithPrefix("query/1/");
    manager.runCheckpointRound();

    EXPECT_EQ(executionOrder, (std::vector<std::string>{"other"}));
}

TEST_F(CheckpointManagerTest, UnregisterSkipsCallbacksThatWereAlreadyCopiedButNotYetInvoked)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    std::promise<void> firstCallbackStartedPromise;
    auto firstCallbackStarted = firstCallbackStartedPromise.get_future();
    std::promise<void> releaseFirstCallbackPromise;
    auto releaseFirstCallback = releaseFirstCallbackPromise.get_future().share();
    std::atomic_bool secondCallbackRan = false;

    manager.registerCallback(
        "prepare_a",
        [&firstCallbackStartedPromise, releaseFirstCallback](uint64_t)
        {
            firstCallbackStartedPromise.set_value();
            releaseFirstCallback.wait();
        },
        CheckpointManager::CallbackPhase::Prepare);
    manager.registerCallback(
        "prepare_b", [&secondCallbackRan](uint64_t) { secondCallbackRan = true; }, CheckpointManager::CallbackPhase::Prepare);

    auto round = std::async(std::launch::async, [&manager] { manager.runCheckpointRound(); });
    ASSERT_EQ(firstCallbackStarted.wait_for(std::chrono::seconds(1)), std::future_status::ready);

    manager.unregisterCallback("prepare_b");
    releaseFirstCallbackPromise.set_value();

    round.get();
    EXPECT_FALSE(secondCallbackRan);
}

TEST_F(CheckpointManagerTest, UnregisterWaitsForActiveCallbackInvocationToFinish)
{
    CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    std::promise<void> callbackStartedPromise;
    auto callbackStarted = callbackStartedPromise.get_future();
    std::promise<void> releaseCallbackPromise;
    auto releaseCallback = releaseCallbackPromise.get_future().share();

    manager.registerCallback(
        "prepare_slow",
        [&callbackStartedPromise, releaseCallback](uint64_t)
        {
            callbackStartedPromise.set_value();
            releaseCallback.wait();
        },
        CheckpointManager::CallbackPhase::Prepare);

    auto round = std::async(std::launch::async, [&manager] { manager.runCheckpointRound(); });
    ASSERT_EQ(callbackStarted.wait_for(std::chrono::seconds(1)), std::future_status::ready);

    auto unregistration = std::async(std::launch::async, [&manager] { manager.unregisterCallback("prepare_slow"); });
    EXPECT_EQ(unregistration.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    releaseCallbackPromise.set_value();
    unregistration.get();
    round.get();
}

TEST_F(CheckpointManagerTest, SchedulerRunsCheckpointRoundsPeriodically)
{
    std::promise<void> secondRoundPromise;
    auto secondRound = secondRoundPromise.get_future();
    std::atomic<int> rounds = 0;
    {
        CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{1}, false);
        manager.registerCallback(
            "count",
            [&](uint64_t)
            {
                if (++rounds == 2)
                {
                    secondRoundPromise.set_value();
                }
            },
            CheckpointManager::CallbackPhase::Prepare);
        ASSERT_EQ(secondRound.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    }
    const auto roundsAfterShutdown = rounds.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(rounds.load(), roundsAfterShutdown);
}

TEST_F(CheckpointManagerTest, RecoveryReadsTheLastCompletedRound)
{
    const auto queryId = QueryId::createLocal(LocalQueryId(generateUUID()));
    {
        CheckpointManager manager(checkpointDirectory, std::chrono::hours{1}, false);
        manager.registerCallback(
            "state",
            [&](const uint64_t round) { manager.persistFile(manager.getRoundDirectory(queryId, round) / "state", std::to_string(round)); },
            CheckpointManager::CallbackPhase::Prepare);
        manager.runCheckpointRound();
        manager.runCheckpointRound();
        manager.registerCallback(
            "failing", [](uint64_t) { throw std::runtime_error("cannot persist state"); }, CheckpointManager::CallbackPhase::Prepare);
        EXPECT_THROW(manager.runCheckpointRound(), std::runtime_error);
    }

    const CheckpointManager recoveringManager(checkpointDirectory, std::chrono::hours{1}, true);
    const auto recoveryRound = recoveringManager.getRecoveryRoundDirectory(queryId);
    ASSERT_TRUE(recoveryRound.has_value());
    EXPECT_EQ(CheckpointManager::loadFile(*recoveryRound / "state"), "2");
    EXPECT_FALSE(recoveringManager.getRecoveryRoundDirectory(QueryId::createLocal(LocalQueryId(generateUUID()))).has_value());
}

TEST_F(CheckpointManagerTest, RecoveryReadsTheLastRoundThatTheQueryTookPartIn)
{
    const auto queryId = QueryId::createLocal(LocalQueryId(generateUUID()));
    {
        CheckpointManager manager(checkpointDirectory, std::chrono::hours{1}, false);
        manager.registerCallback(
            "state",
            [&](const uint64_t round) { manager.persistFile(manager.getRoundDirectory(queryId, round) / "state", std::to_string(round)); },
            CheckpointManager::CallbackPhase::Prepare);
        manager.runCheckpointRound();
        manager.recordCompletedQueryRound(queryId, 1);
        manager.unregisterCallback("state");
        manager.runCheckpointRound();
    }

    const CheckpointManager recoveringManager(checkpointDirectory, std::chrono::hours{1}, true);
    const auto recoveryRound = recoveringManager.getRecoveryRoundDirectory(queryId);
    ASSERT_TRUE(recoveryRound.has_value());
    EXPECT_EQ(CheckpointManager::loadFile(*recoveryRound / "state"), "1");
}

TEST_F(CheckpointManagerTest, PersistFileReplacesContentsWithoutLeavingTempFiles)
{
    const CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, false);
    const auto relativePath = std::filesystem::path{"bundle/plan.pb"};
    const auto checkpointFile = checkpointDirectory / relativePath;

    manager.persistFile(relativePath, "first");
    manager.persistFile(relativePath, "second");

    const auto contents = CheckpointManager::loadFile(checkpointFile);
    ASSERT_TRUE(contents.has_value());
    EXPECT_EQ(*contents, "second");
    EXPECT_FALSE(std::filesystem::exists(checkpointFile.string() + ".tmp"));
}

TEST_F(CheckpointManagerTest, RecoveryReadsFromSnapshotThatLiveWritesDoNotChange)
{
    writeFile(checkpointDirectory / "bundle" / "plan.pb", "checkpointed plan");

    const CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, true);
    manager.persistFile("bundle/plan.pb", "new plan");

    EXPECT_EQ(manager.getRecoveryDirectory(), checkpointDirectory / ".recovery_snapshot");
    EXPECT_EQ(CheckpointManager::loadFile(manager.getRecoveryDirectory() / "bundle" / "plan.pb"), "checkpointed plan");
    EXPECT_EQ(CheckpointManager::loadFile(checkpointDirectory / "bundle" / "plan.pb"), "new plan");
}

TEST_F(CheckpointManagerTest, RecoveryFallsBackToLiveCheckpointDirectoryWhenSnapshotPreparationFails)
{
    writeFile(checkpointDirectory / "bundle" / "plan.pb", "plan");
    const auto unreadableFile = checkpointDirectory / "unreadable.bin";
    writeFile(unreadableFile, "blocked");

    std::error_code errorCode;
    std::filesystem::permissions(unreadableFile, std::filesystem::perms::none, std::filesystem::perm_options::replace, errorCode);
    ASSERT_FALSE(errorCode) << errorCode.message();

    const CheckpointManager manager(checkpointDirectory, std::chrono::milliseconds{0}, true);

    std::filesystem::permissions(
        unreadableFile,
        std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace,
        errorCode);
    ASSERT_FALSE(errorCode) << errorCode.message();

    EXPECT_EQ(manager.getRecoveryDirectory(), checkpointDirectory);
    EXPECT_FALSE(std::filesystem::exists(checkpointDirectory / ".recovery_snapshot"));
    EXPECT_EQ(CheckpointManager::loadFile(manager.getRecoveryDirectory() / "bundle" / "plan.pb"), "plan");
}

}
