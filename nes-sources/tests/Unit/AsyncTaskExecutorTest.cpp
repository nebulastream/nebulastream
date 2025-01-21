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
#include <future>

#include <gtest/gtest.h>

#include <Async/AsyncTaskExecutor.hpp>
#include <Sources/AsyncSource.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
class AsyncTaskExecutorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("AsyncTaskExecutorTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup AsyncTaskExecutorTest class.");
    }

    static void TearDownTestSuite() { NES_INFO("Tear down AsyncTaskExecutorTest class."); }

    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override { BaseUnitTest::TearDown(); }
};


TEST_F(AsyncTaskExecutorTest, TestLifetime)
{
    // const auto executorRef = Sources::AsyncTaskExecutor::getOrCreate();
    // EXPECT_TRUE(executorRef != nullptr);
    // EXPECT_FALSE(executorRef->ioContext().stopped());
    // EXPECT_EQ(executorRef.use_count(), 1);
    //
    // const auto executorRef2 = Sources::AsyncTaskExecutor::getOrCreate();
    // EXPECT_TRUE(executorRef2 != nullptr);
    // EXPECT_EQ(executorRef.get(), executorRef2.get());
    // EXPECT_FALSE(executorRef->ioContext().stopped());
    // EXPECT_EQ(executorRef.use_count(), 2);
}

TEST_F(AsyncTaskExecutorTest, TestDispatch)
{
    // const auto executor = Sources::AsyncTaskExecutor::getOrCreate();
    // EXPECT_FALSE(executor->ioContext().stopped());
    //
    // std::promise<void> taskExecutedPromise;
    // std::future<void> taskExecutedFuture = taskExecutedPromise.get_future();
    //
    // executor->dispatch([&taskExecutedPromise] { taskExecutedPromise.set_value(); });
    //
    // EXPECT_EQ(taskExecutedFuture.wait_for(std::chrono::milliseconds(10)), std::future_status::ready);
    // EXPECT_NO_THROW(taskExecutedFuture.get());
}

}