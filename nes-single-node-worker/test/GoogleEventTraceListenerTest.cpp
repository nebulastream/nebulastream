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

#include <GoogleEventTraceListener.hpp>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <nlohmann/json.hpp>

namespace NES
{

class GoogleEventTraceListenerTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        testTracePath = std::filesystem::temp_directory_path() / "test_trace.json";
    }

    void TearDown() override
    {
        if (std::filesystem::exists(testTracePath)) {
            std::filesystem::remove(testTracePath);
        }
    }

    std::filesystem::path testTracePath;
};

TEST_F(GoogleEventTraceListenerTest, IncompleteEventsAreClosedOnShutdown)
{
    // Create a trace listener
    auto listener = std::make_unique<GoogleEventTraceListener>(testTracePath);
    
    // Simulate some events that start but don't complete
    listener->onEvent(QueryStart{WorkerThreadId(1), QueryId(1)});
    listener->onEvent(PipelineStart{WorkerThreadId(1), QueryId(1), PipelineId(1)});
    listener->onEvent(TaskExecutionStart{WorkerThreadId(1), QueryId(1), PipelineId(1), TaskId(1), 100});
    
    // Gracefully shutdown without completing the events
    listener->gracefulShutdown();
    
    // Verify the trace file exists and contains the incomplete events
    ASSERT_TRUE(std::filesystem::exists(testTracePath));
    
    std::ifstream file(testTracePath);
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    
    // Check that incomplete events were added
    EXPECT_TRUE(content.find("Query 1 (Incomplete)") != std::string::npos);
    EXPECT_TRUE(content.find("Pipeline 1 (Query 1) (Incomplete)") != std::string::npos);
    EXPECT_TRUE(content.find("Task 1 (Incomplete)") != std::string::npos);
    
    // Check that the incomplete flag is set
    EXPECT_TRUE(content.find("\"incomplete\": true") != std::string::npos);
    EXPECT_TRUE(content.find("\"reason\": \"system_shutdown\"") != std::string::npos);
    
    // Verify the JSON is valid
    nlohmann::json traceData = nlohmann::json::parse(content);
    EXPECT_TRUE(traceData.contains("traceEvents"));
}

TEST_F(GoogleEventTraceListenerTest, CompleteEventsAreHandledNormally)
{
    auto listener = std::make_unique<GoogleEventTraceListener>(testTracePath);
    
    // Simulate complete event cycle
    listener->onEvent(QueryStart{WorkerThreadId(1), QueryId(1)});
    listener->onEvent(PipelineStart{WorkerThreadId(1), QueryId(1), PipelineId(1)});
    listener->onEvent(TaskExecutionStart{WorkerThreadId(1), QueryId(1), PipelineId(1), TaskId(1), 100});
    listener->onEvent(TaskExecutionComplete{WorkerThreadId(1), QueryId(1), PipelineId(1), TaskId(1)});
    listener->onEvent(PipelineStop{WorkerThreadId(1), QueryId(1), PipelineId(1)});
    listener->onEvent(QueryStop{WorkerThreadId(1), QueryId(1)});
    
    listener->gracefulShutdown();
    
    // Verify the trace file exists
    ASSERT_TRUE(std::filesystem::exists(testTracePath));
    
    std::ifstream file(testTracePath);
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    
    // Check that normal events are present
    EXPECT_TRUE(content.find("Query 1") != std::string::npos);
    EXPECT_TRUE(content.find("Pipeline 1 (Query 1)") != std::string::npos);
    EXPECT_TRUE(content.find("Task 1 (Pipeline 1, Query 1)") != std::string::npos);
    
    // Check that no incomplete events were added
    EXPECT_TRUE(content.find("(Incomplete)") == std::string::npos);
}

TEST_F(GoogleEventTraceListenerTest, TaskExpiredEventsAreHandled)
{
    auto listener = std::make_unique<GoogleEventTraceListener>(testTracePath);
    
    // Start a task
    listener->onEvent(TaskExecutionStart{WorkerThreadId(1), QueryId(1), PipelineId(1), TaskId(1), 100});
    
    // Task expires instead of completing
    listener->onEvent(TaskExpired{WorkerThreadId(1), QueryId(1), PipelineId(1), TaskId(1)});
    
    listener->gracefulShutdown();
    
    // Verify the trace file exists
    ASSERT_TRUE(std::filesystem::exists(testTracePath));
    
    std::ifstream file(testTracePath);
    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();
    
    // Check that task expired event is present
    EXPECT_TRUE(content.find("Task Expired 1 (Pipeline 1, Query 1)") != std::string::npos);
    
    // Check that no incomplete task event was added (since it was properly expired)
    EXPECT_TRUE(content.find("Task 1 (Incomplete)") == std::string::npos);
}

} // namespace NES 