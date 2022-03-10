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

#include <Runtime/AsyncTaskExecutor.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace NES {
class AsyncTaskExecutorTest : public ::testing::TestWithParam<int> {
  protected:
    Runtime::AsyncTaskExecutorPtr executor{nullptr};

  public:
    void SetUp() { executor = std::make_shared<Runtime::AsyncTaskExecutor>(GetParam()); }

    void TearDown() { executor.reset(); }

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::setupLogging("AsyncTaskExecutorTest.log", NES::LOG_DEBUG); }
};

TEST_P(AsyncTaskExecutorTest, startAndDestroy) { ASSERT_TRUE(executor->destroy()); }

TEST_P(AsyncTaskExecutorTest, submitTask) {
    auto future = executor->runAsync(
        [](auto x, auto y) {
            return x + y;
        },
        1,
        1);
    ASSERT_TRUE(!!future);
    auto sum = future.wait();
    ASSERT_EQ(2, sum);
}

TEST_P(AsyncTaskExecutorTest, submitConcatenatedTasks) {
    try {
        auto future = executor
                          ->runAsync(
                              [](auto x, auto y) {
                                  return x + y;
                              },
                              1,
                              1)
                          .thenAsync([](auto x) {
                              return x + 1;
                          });
        ASSERT_TRUE(!!future);
        auto sum = future.wait();
        ASSERT_EQ(3, sum);
    } catch (std::exception const& expected) {
        NES_DEBUG("<< " << expected.what());
        FAIL();
    }
}

TEST_P(AsyncTaskExecutorTest, submitTaskWithStoppedExecutor) {
    executor->destroy();
    try {
        auto future = executor->runAsync(
            [](auto x, auto y) {
                return x + y;
            },
            1,
            1);
        ASSERT_TRUE(!!future);
        auto sum = future.wait();
        ASSERT_EQ(2, sum);
        FAIL();
    } catch (std::exception const& expected) {
        EXPECT_THAT(expected.what(), testing::HasSubstr("Async Executor is destroyed"));
    } catch (...) {
        FAIL();
    }
}

INSTANTIATE_TEST_CASE_P(AsyncTaskExecutorMTTest, AsyncTaskExecutorTest, ::testing::Values(1, 4, 8));

}// namespace NES