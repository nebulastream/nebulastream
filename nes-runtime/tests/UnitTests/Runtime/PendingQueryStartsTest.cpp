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
#include <barrier>
#include <cstddef>
#include <thread>
#include <tuple>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/PendingQueryStarts.hpp>
#include <Util/UUID.hpp>
#include <QueryId.hpp>

namespace NES
{

class PendingQueryStartsTest : public ::testing::Test
{
protected:
    static QueryId randomQueryId() { return QueryId::createLocal(LocalQueryId(generateUUID())); }

    PendingQueryStarts pendingStarts;
    const QueryId testQueryId = randomQueryId();
};

TEST_F(PendingQueryStartsTest, StopOfUnknownQueryIsNotRecorded)
{
    EXPECT_FALSE(pendingStarts.stop(testQueryId));
}

TEST_F(PendingQueryStartsTest, PendingQueryIsHandedOver)
{
    pendingStarts.add(testQueryId);

    bool handedOver = false;
    EXPECT_TRUE(pendingStarts.finish(testQueryId, [&] { handedOver = true; }));
    EXPECT_TRUE(handedOver);
    /// The engine has the query now, so a stop has to go there.
    EXPECT_FALSE(pendingStarts.stop(testQueryId));
}

TEST_F(PendingQueryStartsTest, StoppedPendingQueryIsNotHandedOver)
{
    pendingStarts.add(testQueryId);
    EXPECT_TRUE(pendingStarts.stop(testQueryId));

    bool handedOver = false;
    EXPECT_FALSE(pendingStarts.finish(testQueryId, [&] { handedOver = true; }));
    EXPECT_FALSE(handedOver);
    EXPECT_FALSE(pendingStarts.stop(testQueryId));
}

TEST_F(PendingQueryStartsTest, StopOnlyAffectsItsOwnQuery)
{
    const auto otherQueryId = randomQueryId();
    pendingStarts.add(testQueryId);
    pendingStarts.add(otherQueryId);
    EXPECT_TRUE(pendingStarts.stop(otherQueryId));

    bool handedOver = false;
    EXPECT_TRUE(pendingStarts.finish(testQueryId, [&] { handedOver = true; }));
    EXPECT_TRUE(handedOver);
}

TEST_F(PendingQueryStartsTest, RemovedQueryIsNotPending)
{
    pendingStarts.add(testQueryId);
    pendingStarts.remove(testQueryId);
    EXPECT_FALSE(pendingStarts.stop(testQueryId));
}

/// A stop and the hand-over race for the same query.
/// Either the stop is recorded and the query is not handed over, or the query is handed over and the stop has to go to the engine.
TEST_F(PendingQueryStartsTest, ConcurrentStopAndFinishAgree)
{
    constexpr size_t rounds = 2000;
    for (size_t round = 0; round < rounds; ++round)
    {
        const auto queryId = randomQueryId();
        pendingStarts.add(queryId);

        std::barrier startTogether{2};
        std::atomic<bool> stopRecorded{false};
        std::atomic<bool> handedOver{false};
        {
            const std::jthread stopper{[&]
                                       {
                                           startTogether.arrive_and_wait();
                                           stopRecorded = pendingStarts.stop(queryId);
                                       }};
            const std::jthread starter{[&]
                                       {
                                           startTogether.arrive_and_wait();
                                           std::ignore = pendingStarts.finish(queryId, [&] { handedOver = true; });
                                       }};
        }
        EXPECT_NE(stopRecorded.load(), handedOver.load());
    }
}
}
