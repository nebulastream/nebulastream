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
#include <cstddef>
#include <future>
#include <latch>
#include <memory>
#include <optional>
#include <random>
#include <stop_token>
#include <thread>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
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

/// Waits on a listener in a thread of its own.
/// Destroying the waiter stops the wait, so a listener that is never woken fails the test instead of hanging it.
class Waiter
{
public:
    explicit Waiter(const BackpressureListener& listener)
        : thread(
              [this, &listener](const std::stop_token& stop)
              {
                  listener.wait(stop);
                  woke.set_value();
              })
    {
    }

    [[nodiscard]] bool wokeWithin(const std::chrono::milliseconds timeout) const
    {
        return done.wait_for(timeout) == std::future_status::ready;
    }

    void requestStop() { thread.request_stop(); }

private:
    std::promise<void> woke;
    std::future<void> done = woke.get_future();
    /// Declared last, so it is joined before the promise is destroyed.
    std::jthread thread;
};

class BackpressureManagerTest : public Testing::BaseUnitTest
{
protected:
    static void SetUpTestSuite() { Logger::setupLogging("BackpressureManagerTest.log", LogLevel::LOG_DEBUG); }

    BackpressureManager manager{
        {SOURCE_A, SOURCE_B},
        {.sourcesOfOrigins = {{JOIN_OUTPUT, {SOURCE_A, SOURCE_B}}}, .inputsOfOrigins = {{JOIN_OUTPUT, {SOURCE_A, SOURCE_B}}}}};
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
    auto sink = manager.newExternalVoter();
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
    auto first = manager.newExternalVoter();
    auto second = manager.newExternalVoter();
    first.applyBackpressureToAll();
    second.applyBackpressureToAll();
    first.releaseBackpressureToAll();
    EXPECT_TRUE(listenerA.isBlocked());
    second.releaseBackpressureToAll();
    EXPECT_FALSE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, SoftVoteBlocksOnlyTheSourcesOfTheOrigin)
{
    auto join = manager.newInternalVoter(JOIN_OUTPUT);
    join.releaseBackpressureTo(SOURCE_B);
    EXPECT_TRUE(join.applyBackpressureTo(SOURCE_A));
    EXPECT_FALSE(join.applyBackpressureTo(SOURCE_A));
    EXPECT_TRUE(listenerA.isBlocked());
    EXPECT_FALSE(listenerB.isBlocked());

    EXPECT_TRUE(join.releaseBackpressureTo(SOURCE_A));
    EXPECT_FALSE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, SoftVoteDoesNotBlockASourceTheWindowWaitsOn)
{
    auto join = manager.newInternalVoter(JOIN_OUTPUT);
    join.applyBackpressureTo(SOURCE_A);
    EXPECT_FALSE(listenerA.isBlocked());

    join.releaseBackpressureTo(SOURCE_B);
    EXPECT_TRUE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, DestroyingAVoterWithdrawsItsVotes)
{
    {
        auto sink = manager.newExternalVoter();
        auto join = manager.newInternalVoter(JOIN_OUTPUT);
        sink.applyBackpressureToAll();
        join.releaseBackpressureTo(SOURCE_B);
        join.applyBackpressureTo(SOURCE_A);
    }
    EXPECT_FALSE(listenerA.isBlocked());
    EXPECT_FALSE(listenerB.isBlocked());
}

TEST_F(BackpressureManagerTest, MoveAssignmentWithdrawsTheVotesOfTheTarget)
{
    auto sink = manager.newExternalVoter();
    sink.applyBackpressureToAll();
    sink = manager.newExternalVoter();
    EXPECT_FALSE(listenerA.isBlocked());
}

TEST_F(BackpressureManagerTest, VotesOutliveTheManagerObject)
{
    auto voter = std::make_unique<ExternalBackpressureVoter>(BackpressureManager({SOURCE_A}, {}).newExternalVoter());
    EXPECT_TRUE(voter->applyBackpressureToAll());
}

TEST_F(BackpressureManagerTest, WaitBlocksUntilReleased)
{
    auto sink = manager.newExternalVoter();
    sink.applyBackpressureToAll();
    const Waiter waiter(listenerA);
    EXPECT_FALSE(waiter.wokeWithin(std::chrono::milliseconds(50)));
    sink.releaseBackpressureToAll();
    EXPECT_TRUE(waiter.wokeWithin(std::chrono::seconds(10)));
}

TEST_F(BackpressureManagerTest, WaitReturnsOnStop)
{
    auto sink = manager.newExternalVoter();
    sink.applyBackpressureToAll();
    Waiter waiter(listenerA);
    EXPECT_FALSE(waiter.wokeWithin(std::chrono::milliseconds(50)));
    waiter.requestStop();
    EXPECT_TRUE(waiter.wokeWithin(std::chrono::seconds(10)));
    /// A stop that does not wake the listener must still let the waiter end.
    sink.releaseBackpressureToAll();
}

/// A voter that is destroyed withdraws its hard vote, which must wake a listener that already waits.
TEST_F(BackpressureManagerTest, DestroyingAVoterWakesAWaitingListener)
{
    std::optional<ExternalBackpressureVoter> sink = manager.newExternalVoter();
    sink->applyBackpressureToAll();
    const Waiter waiter(listenerA);
    EXPECT_FALSE(waiter.wokeWithin(std::chrono::milliseconds(50)));
    sink.reset();
    EXPECT_TRUE(waiter.wokeWithin(std::chrono::seconds(10)));
}

/// The same for the soft votes of an internal voter: its apply and its wait go away together.
TEST_F(BackpressureManagerTest, DestroyingAnInternalVoterWakesAWaitingListener)
{
    std::optional<InternalBackpressureVoter> join = manager.newInternalVoter(JOIN_OUTPUT);
    join->releaseBackpressureTo(SOURCE_B);
    join->applyBackpressureTo(SOURCE_A);
    const Waiter waiter(listenerA);
    EXPECT_FALSE(waiter.wokeWithin(std::chrono::milliseconds(50)));
    join.reset();
    EXPECT_TRUE(waiter.wokeWithin(std::chrono::seconds(10)));
}

/// A move assignment withdraws the votes of the target, which must also wake a listener that already waits.
TEST_F(BackpressureManagerTest, MoveAssignmentWakesAWaitingListener)
{
    auto sink = manager.newExternalVoter();
    sink.applyBackpressureToAll();
    const Waiter waiter(listenerA);
    EXPECT_FALSE(waiter.wokeWithin(std::chrono::milliseconds(50)));
    sink = manager.newExternalVoter();
    EXPECT_TRUE(waiter.wokeWithin(std::chrono::seconds(10)));
}

/// One release must wake every listener that waits on the same source.
TEST_F(BackpressureManagerTest, OneReleaseWakesEveryListenerOfASource)
{
    constexpr size_t listeners = 5;
    auto sink = manager.newExternalVoter();
    sink.applyBackpressureToAll();
    std::vector<BackpressureListener> sameSource(listeners, manager.getListener(SOURCE_A));
    std::vector<std::unique_ptr<Waiter>> waiters;
    for (const auto& listener : sameSource)
    {
        waiters.push_back(std::make_unique<Waiter>(listener));
    }
    for (const auto& waiter : waiters)
    {
        EXPECT_FALSE(waiter->wokeWithin(std::chrono::milliseconds(50)));
    }

    sink.releaseBackpressureToAll();
    for (const auto& waiter : waiters)
    {
        EXPECT_TRUE(waiter->wokeWithin(std::chrono::seconds(10)));
    }
}

/// Voters on several threads change hard and soft votes while listeners call wait on both sources.
/// Each thread has an external and an internal voter, which withdraw their votes when the thread ends.
/// So no source may stay blocked after all threads.
/// The listeners only add concurrent calls of wait. The test does not check that they wake.
TEST_F(BackpressureManagerTest, ConcurrentVotesLeaveNoVoteBehind)
{
    constexpr size_t voterThreads = 8;
    constexpr size_t votesPerThread = 2000;
    {
        std::vector<std::jthread> listenerThreads;
        for (const auto* listener : {&listenerA, &listenerB})
        {
            listenerThreads.emplace_back(
                [listener](const std::stop_token& stop)
                {
                    while (!stop.stop_requested())
                    {
                        listener->wait(stop);
                    }
                });
        }
        std::latch startTogether(voterThreads);
        std::vector<std::jthread> voters;
        for (size_t thread = 0; thread < voterThreads; ++thread)
        {
            voters.emplace_back(
                [&, thread]
                {
                    auto sink = manager.newExternalVoter();
                    auto join = manager.newInternalVoter(JOIN_OUTPUT);
                    std::mt19937 random(thread);
                    std::uniform_int_distribution action(0, 5);
                    startTogether.arrive_and_wait();
                    for (size_t vote = 0; vote < votesPerThread; ++vote)
                    {
                        switch (action(random))
                        {
                            case 0:
                                sink.applyBackpressureToAll();
                                break;
                            case 1:
                                sink.releaseBackpressureToAll();
                                break;
                            case 2:
                                join.applyBackpressureTo(SOURCE_A);
                                break;
                            case 3:
                                join.releaseBackpressureTo(SOURCE_A);
                                break;
                            case 4:
                                join.applyBackpressureTo(SOURCE_B);
                                break;
                            default:
                                join.releaseBackpressureTo(SOURCE_B);
                                break;
                        }
                    }
                });
        }
        voters.clear();
        EXPECT_FALSE(listenerA.isBlocked());
        EXPECT_FALSE(listenerB.isBlocked());
    }
}

/// W1 joins person and auction, W2 joins W1's output with bid. bid is much larger, so W1's output runs ahead of it.
constexpr OriginId PERSON{11};
constexpr OriginId AUCTION{12};
constexpr OriginId BID{13};
constexpr OriginId W1_OUTPUT{14};
constexpr OriginId W2_OUTPUT{15};

class CriticalPathTest : public Testing::BaseUnitTest
{
protected:
    static void SetUpTestSuite() { Logger::setupLogging("BackpressureManagerTest.log", LogLevel::LOG_DEBUG); }

    BackpressureManager manager{
        {PERSON, AUCTION, BID},
        {.sourcesOfOrigins = {{W1_OUTPUT, {PERSON, AUCTION}}, {W2_OUTPUT, {PERSON, AUCTION, BID}}},
         .inputsOfOrigins = {{W1_OUTPUT, {PERSON, AUCTION}}, {W2_OUTPUT, {W1_OUTPUT, BID}}}}};
    BackpressureListener person = manager.getListener(PERSON);
    BackpressureListener auction = manager.getListener(AUCTION);
    BackpressureListener bid = manager.getListener(BID);
    InternalBackpressureVoter w1 = manager.newInternalVoter(W1_OUTPUT);
    InternalBackpressureVoter w2 = manager.newInternalVoter(W2_OUTPUT);
};

TEST_F(CriticalPathTest, AWindowWithoutWaitsWaitsOnAllItsInputs)
{
    w2.applyBackpressureTo(W1_OUTPUT);
    EXPECT_FALSE(person.isBlocked());
    EXPECT_FALSE(auction.isBlocked());
}

TEST_F(CriticalPathTest, UpperWindowOverridesTheReleaseOfAWindowItDoesNotWaitOn)
{
    w1.releaseBackpressureTo(PERSON);
    w2.releaseBackpressureTo(BID);
    w2.applyBackpressureTo(W1_OUTPUT);
    EXPECT_TRUE(person.isBlocked());
    EXPECT_TRUE(auction.isBlocked());
    EXPECT_FALSE(bid.isBlocked());
}

TEST_F(CriticalPathTest, ReleaseCountsOnceTheUpperWindowWaitsOnTheLowerOne)
{
    w1.releaseBackpressureTo(PERSON);
    w2.releaseBackpressureTo(BID);
    w2.applyBackpressureTo(W1_OUTPUT);

    /// bid caught up: W2 now waits on W1, and runs ahead on bid.
    w2.releaseBackpressureTo(W1_OUTPUT);
    w2.applyBackpressureTo(BID);
    EXPECT_FALSE(person.isBlocked());
    EXPECT_FALSE(auction.isBlocked());
    EXPECT_TRUE(bid.isBlocked());
}

TEST_F(CriticalPathTest, ACriticalSourceWakesItsListener)
{
    w1.releaseBackpressureTo(PERSON);
    w2.releaseBackpressureTo(BID);
    w2.applyBackpressureTo(W1_OUTPUT);
    const Waiter waiter(person);
    EXPECT_FALSE(waiter.wokeWithin(std::chrono::milliseconds(50)));
    w2.releaseBackpressureTo(W1_OUTPUT);
    EXPECT_TRUE(waiter.wokeWithin(std::chrono::seconds(10)));
}

TEST_F(CriticalPathTest, DestroyingAnInternalVoterWithdrawsItsWaits)
{
    auto other = manager.newInternalVoter(W1_OUTPUT);
    other.applyBackpressureTo(PERSON);
    w2.releaseBackpressureTo(BID);
    EXPECT_TRUE(person.isBlocked());

    w2 = manager.newInternalVoter(W2_OUTPUT);
    EXPECT_FALSE(person.isBlocked());
}

TEST_F(CriticalPathTest, HardVoteBlocksCriticalSources)
{
    auto sink = manager.newExternalVoter();
    w2.releaseBackpressureTo(BID);
    sink.applyBackpressureToAll();
    EXPECT_TRUE(bid.isBlocked());
}

}

/// W1 joins A and B, and W2 joins A with the output of W1, so A reaches W2 on two paths.
class SharedSourceTest : public Testing::BaseUnitTest
{
protected:
    static void SetUpTestSuite() { Logger::setupLogging("BackpressureManagerTest.log", LogLevel::LOG_DEBUG); }

    static constexpr OriginId A{21};
    static constexpr OriginId B{22};
    static constexpr OriginId W1{23};
    static constexpr OriginId W2{24};

    BackpressureManager manager{
        {A, B}, {.sourcesOfOrigins = {{W1, {A, B}}, {W2, {A, B}}}, .inputsOfOrigins = {{W1, {A, B}}, {W2, {A, W1}}}}};
    BackpressureListener a = manager.getListener(A);
    BackpressureListener b = manager.getListener(B);
    InternalBackpressureVoter w1 = manager.newInternalVoter(W1);
    InternalBackpressureVoter w2 = manager.newInternalVoter(W2);
};

/// W1 waits on B and holds A, while W2 waits on W1 and holds A.
/// B stays on the critical path, so the two windows cannot block both sources.
TEST_F(SharedSourceTest, TheSourceTheLowerWindowWaitsOnStaysCritical)
{
    w1.releaseBackpressureTo(B);
    w1.applyBackpressureTo(A);
    w2.releaseBackpressureTo(W1);
    w2.applyBackpressureTo(A);
    EXPECT_TRUE(a.isBlocked());
    EXPECT_FALSE(b.isBlocked());
}

/// The release of W2 on A cancels its own apply on W1 at A, and makes A critical.
TEST_F(SharedSourceTest, ReleaseWinsWithinOneVoter)
{
    w2.releaseBackpressureTo(A);
    w2.applyBackpressureTo(W1);
    EXPECT_FALSE(a.isBlocked());
    EXPECT_TRUE(b.isBlocked());
}
}
