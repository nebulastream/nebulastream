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
#include <latch>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#include <gtest/gtest.h>
#include <Callback.hpp>

using namespace NES;

class CallbackTest : public ::testing::Test
{
};

TEST_F(CallbackTest, BasicCallback)
{
    std::atomic<bool> callbackExecuted{false};

    {
        auto [owner, ref] = Callback::create();
        owner.setCallback([&callbackExecuted] { callbackExecuted = true; });

        EXPECT_FALSE(callbackExecuted);
    }

    EXPECT_TRUE(callbackExecuted);
}

TEST_F(CallbackTest, OwnerDestroyedFirst)
{
    std::atomic<bool> callbackExecuted{false};

    CallbackRef ref;
    {
        auto [owner, r] = Callback::create();
        owner.setCallback([&callbackExecuted] { callbackExecuted = true; });
        ref = r;
    }

    EXPECT_FALSE(callbackExecuted);

    ref = CallbackRef{};
    EXPECT_FALSE(callbackExecuted);
}

TEST_F(CallbackTest, MultipleRefs)
{
    std::atomic<int> callbackCount{0};

    auto [owner, ref] = Callback::create();
    owner.setCallback([&callbackCount] { ++callbackCount; });

    std::vector<CallbackRef> refs;
    refs.reserve(5);
    for (int i = 0; i < 5; ++i)
    {
        refs.push_back(ref);
    }

    {
        const CallbackRef temp = std::move(ref);
    }
    EXPECT_EQ(callbackCount, 0);

    for (int i = 0; i < 4; ++i)
    {
        refs.pop_back();
        EXPECT_EQ(callbackCount, 0);
    }

    refs.pop_back();
    EXPECT_EQ(callbackCount, 1);
}

TEST_F(CallbackTest, ConcurrentDestruction)
{
    /// This test verifies the key invariant: when the owner destructor returns,
    /// either the callback never started, or it has fully completed.

    std::latch callbackStarted{1};
    std::latch callbackMayFinish{1};
    std::atomic<bool> callbackFinished{false};

    auto [owner, ref] = Callback::create();
    owner.setCallback(
        [&]
        {
            callbackStarted.count_down();
            callbackMayFinish.wait();
            callbackFinished = true;
        });

    std::thread ownerThread(
        [&, o = std::move(owner)]() mutable
        {
            callbackStarted.wait();
            callbackMayFinish.count_down();
            o = CallbackOwner{}; /// Should block until callback finishes
        });

    {
        const CallbackRef temp = std::move(ref);
    }

    ownerThread.join();
    EXPECT_TRUE(callbackFinished);
}

TEST_F(CallbackTest, TwoPhaseInit)
{
    struct MyPlan
    {
        int id = 0;
        CallbackOwner onComplete;
        std::vector<CallbackRef> nodeRefs;
    };

    std::atomic<bool> callbackExecuted{false};
    int capturedId = -1;

    {
        auto [owner, ref] = Callback::create();

        MyPlan plan;
        plan.id = 42;
        plan.onComplete = std::move(owner);

        plan.nodeRefs.push_back(ref);
        plan.nodeRefs.push_back(ref);

        plan.onComplete.setCallback(
            [&callbackExecuted, &capturedId, &plan]
            {
                capturedId = plan.id;
                callbackExecuted = true;
            });

        {
            const CallbackRef temp = std::move(ref);
        }

        EXPECT_FALSE(callbackExecuted);

        plan.nodeRefs.clear();
    }

    EXPECT_TRUE(callbackExecuted);
    EXPECT_EQ(capturedId, 42);
}

TEST_F(CallbackTest, CallbackDestroysOwnerNoDeadlock)
{
    std::atomic<bool> callbackExecuted{false};
    std::atomic<bool> ownerDestroyed{false};

    auto ownerPtr = std::make_unique<CallbackOwner>();

    auto [owner, ref] = Callback::create();
    *ownerPtr = std::move(owner);

    ownerPtr->setCallback(
        [&callbackExecuted, &ownerDestroyed, &ownerPtr]
        {
            ownerPtr.reset();
            ownerDestroyed = true;
            callbackExecuted = true;
        });

    {
        const CallbackRef temp = std::move(ref);
    }

    EXPECT_TRUE(callbackExecuted);
    EXPECT_TRUE(ownerDestroyed);
}

TEST_F(CallbackTest, MoveAssignmentClearsOldCallback)
{
    std::atomic<int> callback1Count{0};
    std::atomic<int> callback2Count{0};

    auto [owner1, ref1] = Callback::create();
    owner1.setCallback([&callback1Count] { ++callback1Count; });

    auto [owner2, ref2] = Callback::create();
    owner2.setCallback([&callback2Count] { ++callback2Count; });

    owner1 = std::move(owner2);

    {
        const CallbackRef temp = std::move(ref1);
    }
    EXPECT_EQ(callback1Count, 0);

    {
        const CallbackRef temp = std::move(ref2);
    }
    EXPECT_EQ(callback2Count, 1);
}
