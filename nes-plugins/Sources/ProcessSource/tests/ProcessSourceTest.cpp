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
#include <cstdint>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <unistd.h>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ProcessSource.hpp>

/// NOLINTBEGIN(readability-magic-numbers) -- test values are intentional

namespace NES
{
namespace
{
constexpr uint32_t BUFFER_SIZE = 4096;
constexpr uint64_t SOURCE_ID = 7;
}

class ProcessSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ProcessSourceTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create(
            64ULL * BUFFER_SIZE, 0.5, BufferAlignment{64}, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
    }

    /// Fills one buffer and returns its content. Returns an empty string on end of stream.
    std::string fill(ProcessSource& source) const { return fill(source, std::stop_token{}); }

    std::string fill(ProcessSource& source, const std::stop_token& stopToken) const
    {
        auto buffer = bufferManager->getBufferBlocking();
        const auto result = source.fillTupleBuffer(buffer, stopToken);
        if (result.isEoS())
        {
            return {};
        }
        const auto bytes = buffer.getAvailableMemoryArea<char>().first(result.getNumberOfBytes());
        return std::string{bytes.begin(), bytes.end()};
    }

    std::shared_ptr<BufferManager> bufferManager;
};

TEST_F(ProcessSourceTest, ReadsStandardOutputOfEachInvocation)
{
    ProcessSource source{{.command = "echo \"run $INVOCATION\"", .refreshIntervalMs = 20, .flushIntervalMs = 10}, SOURCE_ID};
    source.open(bufferManager);
    EXPECT_EQ(fill(source), "run 1\n");
    EXPECT_EQ(fill(source), "run 2\n");
    EXPECT_EQ(fill(source), "run 3\n");
    source.close();
}

TEST_F(ProcessSourceTest, ExportsWorkerPidAndSourceId)
{
    ProcessSource source{
        {.command = "echo \"$PID $pid $WORKER_PID $SOURCE_ID\"", .refreshIntervalMs = 1000, .flushIntervalMs = 10}, SOURCE_ID};
    source.open(bufferManager);
    const auto pid = std::to_string(getpid());
    EXPECT_EQ(fill(source), pid + " " + pid + " " + pid + " " + std::to_string(SOURCE_ID) + "\n");
    source.close();
}

TEST_F(ProcessSourceTest, SupportsShellPipelinesAndIgnoresStandardError)
{
    ProcessSource source{
        {.command = "echo ignored >&2; printf 'b\\na\\n' | sort", .refreshIntervalMs = 1000, .flushIntervalMs = 50}, SOURCE_ID};
    source.open(bufferManager);
    EXPECT_EQ(fill(source), "a\nb\n");
    source.close();
}

TEST_F(ProcessSourceTest, SplitsOutputLargerThanOneBuffer)
{
    ProcessSource source{{.command = "head -c 6000 /dev/zero | tr '\\0' x", .refreshIntervalMs = 60000, .flushIntervalMs = 200}, SOURCE_ID};
    source.open(bufferManager);
    const auto first = fill(source);
    const auto second = fill(source);
    EXPECT_EQ(first.size(), BUFFER_SIZE);
    EXPECT_EQ(first.size() + second.size(), 6000);
    source.close();
}

TEST_F(ProcessSourceTest, FailingCommandDoesNotStopTheSource)
{
    ProcessSource source{
        {.command = "[ \"$INVOCATION\" -gt 1 ] || exit 3; echo recovered", .refreshIntervalMs = 20, .flushIntervalMs = 10}, SOURCE_ID};
    source.open(bufferManager);
    EXPECT_EQ(fill(source), "recovered\n");
    source.close();
}

TEST_F(ProcessSourceTest, StopRequestTerminatesRunningCommand)
{
    ProcessSource source{{.command = "sleep 60", .refreshIntervalMs = 1000, .flushIntervalMs = 10}, SOURCE_ID};
    source.open(bufferManager);
    std::stop_source stopSource;
    const std::jthread stopper{[&stopSource]
                               {
                                   std::this_thread::sleep_for(std::chrono::milliseconds{100});
                                   stopSource.request_stop();
                               }};
    const auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(fill(source, stopSource.get_token()), "");
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds{10});
    source.close();
}

}

/// NOLINTEND(readability-magic-numbers)
