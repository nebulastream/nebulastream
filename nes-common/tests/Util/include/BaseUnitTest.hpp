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

#pragma once
#include <atomic>
#include <cstdint>
#include <future>
#include <thread>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace NES::Testing
{


/// Defines the SKIP_IF_TSAN macro, which skips the current test if the test was built with tsan.
/// This is usually necessary if the test uses gtests EXPECT_DEATH_DEBUG
#if defined(__has_feature) && __has_feature(thread_sanitizer)
    #define SKIP_IF_TSAN() \
        do \
        { \
            GTEST_SKIP() << "Test is disabled when running with TSAN"; \
        } while (0)
#else
    #define SKIP_IF_TSAN()
#endif


/// Test with EXPECT_DEATH_DEBUG and ASSERT_DEATH_DEBUG for testing correct behaviour when asserts are disabled
#if defined(NDEBUG)
    /// In Release: Death-Tests as No-Op
    #define EXPECT_DEATH_DEBUG(statement, regex) \
        do \
        { \
            try \
            { \
                statement; \
            } \
            catch (...) \
            { \
            } \
        } while (0)
    #define ASSERT_DEATH_DEBUG(statement, regex) \
        do \
        { \
            try \
            { \
                statement; \
            } \
            catch (...) \
            { \
            } \
        } while (0)
#else
    /// In Debug-Modus: Use regular EXPECT_DEATH
    #define EXPECT_DEATH_DEBUG(statement, regex) EXPECT_DEATH(statement, regex)
    #define ASSERT_DEATH_DEBUG(statement, regex) ASSERT_DEATH(statement, regex)
#endif

#define ASSERT_EXCEPTION_ERRORCODE(statement, errorCode) \
    try \
    { \
        statement; \
        FAIL(); \
    } \
    catch (const Exception& ex) \
    { \
        ASSERT_EQ(ex.code(), errorCode); \
    }

namespace detail
{
class TestWaitingHelper
{
public:
    TestWaitingHelper();
    void startWaitingThread(std::string testName);
    void completeTest();
    void failTest();

private:
    std::unique_ptr<std::thread> waitThread;
    std::shared_ptr<std::promise<bool>> testCompletion;
    std::atomic<bool> testCompletionSet{false};
    static constexpr uint64_t WAIT_TIME_SETUP = 5;
};

/**
 * @brief This class is used to generate source names that include an ascending counter.
 */
class TestSourceNameHelper
{
public:
    TestSourceNameHelper();

    /**
     * @brief Returns the string "source" concatenated with the source counter. The latter is then increased.
     * @return std::string
     */
    std::string operator*();

private:
    uint64_t srcCnt;
};
}

class BaseUnitTest : public testing::Test, public detail::TestWaitingHelper
{
    /// This deleter is used to create a shared_ptr that does not delete the object.
    struct Deleter
    {
        void operator()(void*) const { }
    };

public:
    void SetUp() override;
    void TearDown() override;

    detail::TestSourceNameHelper srcName;
};

}
