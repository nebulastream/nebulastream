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
#include <filesystem>
#include <fstream>
#include <future>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <Runtime/CheckpointManager.hpp>

namespace NES
{

class CheckpointManagerTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        checkpointDirectory = std::filesystem::temp_directory_path()
            / ("checkpoint-manager-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        CheckpointManager::initialize(checkpointDirectory, std::chrono::milliseconds{0}, false);
    }

    void TearDown() override
    {
        CheckpointManager::shutdown();
        std::error_code ec;
        std::filesystem::remove_all(checkpointDirectory, ec);
    }

    std::filesystem::path checkpointDirectory;
};

TEST_F(CheckpointManagerTest, RunsPrepareCallbacksBeforeCommitCallbacksInStableOrder)
{
    std::vector<std::string> executionOrder;

    CheckpointManager::registerCallback(
        "prepare_b", [&executionOrder] { executionOrder.emplace_back("prepare_b"); }, CheckpointManager::CallbackPhase::Prepare);
    CheckpointManager::registerCallback(
        "commit_z", [&executionOrder] { executionOrder.emplace_back("commit_z"); }, CheckpointManager::CallbackPhase::Commit);
    CheckpointManager::registerCallback(
        "prepare_a", [&executionOrder] { executionOrder.emplace_back("prepare_a"); }, CheckpointManager::CallbackPhase::Prepare);
    CheckpointManager::registerCallback(
        "commit_a", [&executionOrder] { executionOrder.emplace_back("commit_a"); }, CheckpointManager::CallbackPhase::Commit);

    CheckpointManager::runCallbacksOnce();

    EXPECT_EQ(
        executionOrder,
        (std::vector<std::string>{"prepare_a", "prepare_b", "commit_a", "commit_z"}));
}

TEST_F(CheckpointManagerTest, ReRegisteringCallbackMovesItBetweenPhases)
{
    std::vector<std::string> executionOrder;

    CheckpointManager::registerCallback(
        "shared", [&executionOrder] { executionOrder.emplace_back("prepare"); }, CheckpointManager::CallbackPhase::Prepare);
    CheckpointManager::registerCallback(
        "shared", [&executionOrder] { executionOrder.emplace_back("commit"); }, CheckpointManager::CallbackPhase::Commit);

    CheckpointManager::runCallbacksOnce();

    EXPECT_EQ(executionOrder, (std::vector<std::string>{"commit"}));
}

TEST_F(CheckpointManagerTest, UnregisterSkipsCallbacksThatWereAlreadyCopiedButNotYetInvoked)
{
    std::promise<void> firstCallbackStartedPromise;
    auto firstCallbackStarted = firstCallbackStartedPromise.get_future();
    std::promise<void> releaseFirstCallbackPromise;
    auto releaseFirstCallback = releaseFirstCallbackPromise.get_future().share();
    std::atomic_bool secondCallbackRan = false;

    CheckpointManager::registerCallback(
        "prepare_a",
        [&firstCallbackStartedPromise, releaseFirstCallback]
        {
            firstCallbackStartedPromise.set_value();
            releaseFirstCallback.wait();
        },
        CheckpointManager::CallbackPhase::Prepare);
    CheckpointManager::registerCallback(
        "prepare_b",
        [&secondCallbackRan] { secondCallbackRan.store(true, std::memory_order_release); },
        CheckpointManager::CallbackPhase::Prepare);

    auto runFuture = std::async(std::launch::async, [] { CheckpointManager::runCallbacksOnce(); });
    ASSERT_EQ(firstCallbackStarted.wait_for(std::chrono::seconds(1)), std::future_status::ready);

    CheckpointManager::unregisterCallback("prepare_b");
    releaseFirstCallbackPromise.set_value();

    runFuture.get();
    EXPECT_FALSE(secondCallbackRan.load(std::memory_order_acquire));
}

TEST_F(CheckpointManagerTest, UnregisterWaitsForActiveCallbackInvocationToFinish)
{
    std::promise<void> callbackStartedPromise;
    auto callbackStarted = callbackStartedPromise.get_future();
    std::promise<void> releaseCallbackPromise;
    auto releaseCallback = releaseCallbackPromise.get_future().share();

    CheckpointManager::registerCallback(
        "prepare_slow",
        [&callbackStartedPromise, releaseCallback]
        {
            callbackStartedPromise.set_value();
            releaseCallback.wait();
        },
        CheckpointManager::CallbackPhase::Prepare);

    auto runFuture = std::async(std::launch::async, [] { CheckpointManager::runCallbacksOnce(); });
    ASSERT_EQ(callbackStarted.wait_for(std::chrono::seconds(1)), std::future_status::ready);

    auto unregisterFuture = std::async(std::launch::async, [] { CheckpointManager::unregisterCallback("prepare_slow"); });
    EXPECT_EQ(unregisterFuture.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);

    releaseCallbackPromise.set_value();

    unregisterFuture.get();
    runFuture.get();
}

TEST_F(CheckpointManagerTest, PersistFileReplacesContentsWithoutLeavingTempFiles)
{
    const auto relativePath = std::string{"bundle/plan.pb"};
    const auto checkpointFile = checkpointDirectory / relativePath;
    const auto tempCheckpointFile = checkpointFile.string() + ".tmp";

    CheckpointManager::persistFile(relativePath, "first");
    CheckpointManager::persistFile(relativePath, "second");

    const auto contents = CheckpointManager::loadFile(checkpointFile);
    ASSERT_TRUE(contents.has_value());
    EXPECT_EQ(*contents, "second");
    EXPECT_FALSE(std::filesystem::exists(tempCheckpointFile));
}

TEST_F(CheckpointManagerTest, RecoveryFallsBackToLiveCheckpointDirectoryWhenSnapshotPreparationFails)
{
    const auto readableFile = checkpointDirectory / "bundle" / "plan.pb";
    std::filesystem::create_directories(readableFile.parent_path());
    {
        std::ofstream out(readableFile, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.is_open());
        out << "plan";
    }

    const auto unreadableFile = checkpointDirectory / "unreadable.bin";
    {
        std::ofstream out(unreadableFile, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.is_open());
        out << "blocked";
    }

    std::error_code ec;
    std::filesystem::permissions(unreadableFile, std::filesystem::perms::none, std::filesystem::perm_options::replace, ec);
    ASSERT_FALSE(ec) << ec.message();

    CheckpointManager::initialize(checkpointDirectory, std::chrono::milliseconds{0}, true);

    std::filesystem::permissions(
        unreadableFile,
        std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
        std::filesystem::perm_options::replace,
        ec);
    ASSERT_FALSE(ec) << ec.message();

    EXPECT_EQ(CheckpointManager::getCheckpointRecoveryDirectory(), checkpointDirectory);
    EXPECT_FALSE(std::filesystem::exists(checkpointDirectory / ".recovery_snapshot"));

    const auto contents = CheckpointManager::loadFile(CheckpointManager::getCheckpointRecoveryDirectory() / "bundle" / "plan.pb");
    ASSERT_TRUE(contents.has_value());
    EXPECT_EQ(*contents, "plan");
}

}
