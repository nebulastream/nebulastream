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

#include <BackpressureManager.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <stop_token>
#include <Identifiers/Identifiers.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
constexpr OriginId SOURCE_A{1};
constexpr OriginId SOURCE_B{2};
/// Produced by an operator above both sources, e.g., a join.
constexpr OriginId JOIN_OUTPUT{3};

class BackpressureManagerTest : public ::testing::Test
{
protected:
    BackpressureManager manager{{SOURCE_A, SOURCE_B}, {{JOIN_OUTPUT, {SOURCE_A, SOURCE_B}}}};
    BackpressureListener listenerA = manager.getListener(SOURCE_A);
    BackpressureListener listenerB = manager.getListener(SOURCE_B);
};

TEST_F(BackpressureManagerTest, SourcesStartUnblocked)
{
    EXPECT_FALSE(listenerA.isBlocked());
    EXPECT_FALSE(listenerB.isBlocked());
}

TEST_F(BackpressureManagerTest, HardVoteBlocksAllSources)
{
    auto sink = manager.newVoter();
    EXPECT_TRUE(sink.applyBackpressureToAll());
    EXPECT_FALSE(sink.applyBackpressureToAll());
    EXPECT_TRUE(listenerA.isBlocked());
    EXPECT_TRUE(listenerB.isBlocked());

    EXPECT_TRUE(sink.releaseBackpressureToAll());
    EXPECT_FALSE(sink.releaseBackpressureToAll());
    EXPECT_FALSE(listenerA.isBlocked());
    EXPECT_FALSE(listenerB.isBlocked());
}

TEST_F(BackpressureManagerTest, HardVotesAreOred)
{
    auto first = manager.newVoter();
    auto second = manager.newVoter();
    first.applyBackpressureToAll();
    second.applyBackpressureToAll();
    first.releaseBackpressureToAll();
    EXPECT_TRUE(listenerA.isBlocked());
    second.releaseBackpressureToAll();
    EXPECT_FALSE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, SoftVoteBlocksOnlyTheSourcesOfTheOrigin)
{
    auto window = manager.newVoter();
    EXPECT_TRUE(window.applyBackpressureTo(SOURCE_A));
    EXPECT_FALSE(window.applyBackpressureTo(SOURCE_A));
    EXPECT_TRUE(listenerA.isBlocked());
    EXPECT_FALSE(listenerB.isBlocked());

    EXPECT_TRUE(window.withdrawVote(SOURCE_A));
    EXPECT_FALSE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, SoftVoteOfAnOperatorOriginBlocksAllItsSources)
{
    auto window = manager.newVoter();
    window.applyBackpressureTo(JOIN_OUTPUT);
    EXPECT_TRUE(listenerA.isBlocked());
    EXPECT_TRUE(listenerB.isBlocked());
}

TEST_F(BackpressureManagerTest, SoftReleaseVetoesSoftApply)
{
    auto leading = manager.newVoter();
    auto waiting = manager.newVoter();
    leading.applyBackpressureTo(SOURCE_A);
    waiting.releaseBackpressureTo(SOURCE_A);
    EXPECT_FALSE(listenerA.isBlocked());

    waiting.withdrawVote(SOURCE_A);
    EXPECT_TRUE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, SoftReleaseDoesNotVetoHardVote)
{
    auto sink = manager.newVoter();
    auto window = manager.newVoter();
    sink.applyBackpressureToAll();
    window.releaseBackpressureTo(SOURCE_A);
    EXPECT_TRUE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, ReleaseWinsWithinOneVoter)
{
    auto window = manager.newVoter();
    window.applyBackpressureTo(JOIN_OUTPUT);
    window.releaseBackpressureTo(SOURCE_B);
    EXPECT_TRUE(listenerA.isBlocked());
    EXPECT_FALSE(listenerB.isBlocked());

    window.withdrawVote(SOURCE_B);
    EXPECT_TRUE(listenerB.isBlocked());
}

TEST_F(BackpressureManagerTest, DestroyingAVoterWithdrawsItsVotes)
{
    {
        auto sink = manager.newVoter();
        auto window = manager.newVoter();
        sink.applyBackpressureToAll();
        window.applyBackpressureTo(SOURCE_A);
    }
    EXPECT_FALSE(listenerA.isBlocked());
    EXPECT_FALSE(listenerB.isBlocked());
}

TEST_F(BackpressureManagerTest, MoveAssignmentWithdrawsTheVotesOfTheTarget)
{
    auto sink = manager.newVoter();
    sink.applyBackpressureToAll();
    sink = manager.newVoter();
    EXPECT_FALSE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, VotesOutliveTheManagerObject)
{
    auto voter = std::make_unique<BackpressureVoter>(BackpressureManager({SOURCE_A}, {}).newVoter());
    EXPECT_TRUE(voter->applyBackpressureToAll());
}

TEST_F(BackpressureManagerTest, WaitBlocksUntilReleased)
{
    auto sink = manager.newVoter();
    sink.applyBackpressureToAll();
    auto waited = std::async(std::launch::async, [&] { listenerA.wait(std::stop_token{}); });
    EXPECT_EQ(waited.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    sink.releaseBackpressureToAll();
    EXPECT_EQ(waited.wait_for(std::chrono::seconds(10)), std::future_status::ready);
}

TEST_F(BackpressureManagerTest, WaitReturnsOnStop)
{
    auto sink = manager.newVoter();
    sink.applyBackpressureToAll();
    std::stop_source stop;
    auto waited = std::async(std::launch::async, [&] { listenerA.wait(stop.get_token()); });
    EXPECT_EQ(waited.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
    stop.request_stop();
    EXPECT_EQ(waited.wait_for(std::chrono::seconds(10)), std::future_status::ready);
}

TEST_F(BackpressureManagerTest, VoteForAnUnknownOriginFails)
{
    auto window = manager.newVoter();
    EXPECT_DEATH_DEBUG(window.applyBackpressureTo(OriginId(99)), "");
}

}
}
