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
#include <cstdint>
#include <gtest/gtest.h>

#include <Nautilus/Interface/Rollover/RolloverIndex.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace NES::Nautilus::Interface
{

class RolloverIndexTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("RolloverIndexTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup RolloverIndexTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down RolloverIndexTest class."); }
};

namespace
{

static bool g_callbackCalled = false;
static unsigned long g_rolloverValueObserved = 0;
}

TEST_F(RolloverIndexTest, ConstructorInitializesIndexAndRolloverValue)
{
    g_callbackCalled = false;
    g_rolloverValueObserved = 5;

    RolloverIndex::Callback cb = []()
    {
        g_callbackCalled = true;
        NES_INFO("Rollover callback triggered at value {}", g_rolloverValueObserved);
    };

    RolloverIndex idx(5UL, cb);

    EXPECT_EQ(idx.getIndex(), 0UL);
    EXPECT_EQ(idx.getRolloverValue(), 5UL);
    EXPECT_FALSE(g_callbackCalled);
}

TEST_F(RolloverIndexTest, SetIndexBelowRolloverDoesNotTriggerCallback)
{
    g_callbackCalled = false;
    g_rolloverValueObserved = 3;

    RolloverIndex::Callback cb = []()
    {
        g_callbackCalled = true;
        NES_INFO("Rollover callback triggered at value {}", g_rolloverValueObserved);
    };

    RolloverIndex idx(3UL, cb);

    idx.setIndex(1UL);

    EXPECT_EQ(idx.getIndex(), 1UL);
    EXPECT_FALSE(g_callbackCalled);
}

TEST_F(RolloverIndexTest, SetIndexAtRolloverResetsIndexAndTriggersCallback)
{
    g_callbackCalled = false;
    g_rolloverValueObserved = 3;

    RolloverIndex::Callback cb = []()
    {
        g_callbackCalled = true;
        NES_INFO("Rollover callback triggered at value {}", g_rolloverValueObserved);
    };

    RolloverIndex idx(3UL, cb);

    idx.setIndex(3UL);

    EXPECT_EQ(idx.getIndex(), 0UL);
    EXPECT_TRUE(g_callbackCalled);
}

TEST_F(RolloverIndexTest, IncrementFromBelowRolloverDoesNotTriggerCallback)
{
    g_callbackCalled = false;
    g_rolloverValueObserved = 4;

    RolloverIndex::Callback cb = []()
    {
        g_callbackCalled = true;
        NES_INFO("Rollover callback triggered at value {}", g_rolloverValueObserved);
    };

    RolloverIndex idx(4UL, cb);

    idx.setIndex(2UL);
    idx.increment();

    EXPECT_EQ(idx.getIndex(), 3UL);
    EXPECT_FALSE(g_callbackCalled);
}

TEST_F(RolloverIndexTest, IncrementAtRolloverResetsIndexAndTriggersCallback)
{
    g_callbackCalled = false;
    g_rolloverValueObserved = 4;

    RolloverIndex::Callback cb = []()
    {
        g_callbackCalled = true;
        NES_INFO("Rollover callback triggered at value {}", g_rolloverValueObserved);
    };

    RolloverIndex idx(4UL, cb);

    idx.setIndex(3UL);
    idx.increment();

    EXPECT_EQ(idx.getIndex(), 0UL);
    EXPECT_TRUE(g_callbackCalled);
}

TEST_F(RolloverIndexTest, SetRolloverValueUpdatesLimitButKeepsIndex)
{
    g_callbackCalled = false;
    g_rolloverValueObserved = 10;

    RolloverIndex::Callback cb = []()
    {
        g_callbackCalled = true;
        NES_INFO("Rollover callback triggered at value {}", g_rolloverValueObserved);
    };

    RolloverIndex idx(5UL, cb);

    idx.setIndex(2UL);
    idx.setRolloverValue(10UL);

    EXPECT_EQ(idx.getRolloverValue(), 10UL);
    EXPECT_EQ(idx.getIndex(), 2UL);
    EXPECT_FALSE(g_callbackCalled);
}

}
