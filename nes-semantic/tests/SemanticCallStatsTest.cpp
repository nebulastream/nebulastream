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
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>
#include <unistd.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <SemanticCallStats.hpp>

namespace NES
{
namespace
{

std::vector<std::string> readLines(const std::filesystem::path& path)
{
    std::ifstream in{path};
    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);)
    {
        lines.push_back(line);
    }
    return lines;
}

class SemanticCallStatsTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        path = std::filesystem::temp_directory_path()
            / ("semantic_call_log_" + std::to_string(::getpid()) + "_" + ::testing::UnitTest::GetInstance()->current_test_info()->name()
               + ".jsonl");
        std::filesystem::remove(path);
        SemanticCallStats::instance().reportAndReset("setup");
    }

    void TearDown() override
    {
        SemanticCallStats::instance().setCallLogPath(std::nullopt);
        std::filesystem::remove(path);
    }

    static void exchange(const SemanticCallRecord& record)
    {
        const auto now = std::chrono::steady_clock::now();
        SemanticCallStats::instance().recordCall(now, now + std::chrono::milliseconds{5}, record.status != 200);
        SemanticCallStats::instance().recordExchange(record);
    }

    std::filesystem::path path;
};

const SemanticCallRecord student{
    .startNs = 1795166844198000000,
    .endNs = 1795166844400500000,
    .status = 200,
    .promptTokens = 261,
    .completionTokens = 1,
    .rows = 1,
    .model = "student-model",
    .role = "student"};

const SemanticCallRecord failed{
    .startNs = 1795166845000000000,
    .endNs = 1795166845100000000,
    .status = 0,
    .promptTokens = std::nullopt,
    .completionTokens = std::nullopt,
    .rows = 4,
    .model = "teacher-model",
    .role = ""};

TEST_F(SemanticCallStatsTest, FormatsOneJsonObjectPerRecord)
{
    const auto line = nlohmann::json::parse(SemanticCallStats::formatCallRecord(student, "SEM_CALL"));
    EXPECT_EQ(line["context"], "SEM_CALL");
    EXPECT_EQ(line["start_ns"], 1795166844198000000);
    EXPECT_EQ(line["end_ns"], 1795166844400500000);
    EXPECT_DOUBLE_EQ(line["latency_ms"].get<double>(), 202.5);
    EXPECT_EQ(line["status"], 200);
    EXPECT_EQ(line["prompt_tokens"], 261);
    EXPECT_EQ(line["completion_tokens"], 1);
    EXPECT_EQ(line["rows"], 1);
    EXPECT_EQ(line["model"], "student-model");
    EXPECT_EQ(line["role"], "student");
    EXPECT_EQ(SemanticCallStats::formatCallRecord(student, "SEM_CALL").find('\n'), std::string::npos);
}

TEST_F(SemanticCallStatsTest, NoResponseNoUsageAndNoCascadeAreNull)
{
    const auto line = nlohmann::json::parse(SemanticCallStats::formatCallRecord(failed, "SEM_CALL"));
    EXPECT_TRUE(line["status"].is_null());
    EXPECT_TRUE(line["prompt_tokens"].is_null());
    EXPECT_TRUE(line["completion_tokens"].is_null());
    EXPECT_TRUE(line["role"].is_null());
    EXPECT_EQ(line["rows"], 4);
}

TEST_F(SemanticCallStatsTest, WritesTheRecordsOnceAtTheEndAndAppends)
{
    {
        std::ofstream earlier{path};
        earlier << "{\"earlier\":true}\n";
    }
    SemanticCallStats::instance().setCallLogPath(path.string());
    exchange(student);
    exchange(failed);
    EXPECT_EQ(readLines(path).size(), 1U) << "nothing is written while the query runs";

    SemanticCallStats::instance().reportAndReset("SEM_CALL");
    auto lines = readLines(path);
    ASSERT_EQ(lines.size(), 3U);
    EXPECT_EQ(lines[0], "{\"earlier\":true}");
    EXPECT_EQ(lines[1], SemanticCallStats::formatCallRecord(student, "SEM_CALL"));
    EXPECT_EQ(lines[2], SemanticCallStats::formatCallRecord(failed, "SEM_CALL"));

    /// The records are gone after the report: a second run appends only its own.
    SemanticCallStats::instance().reportAndReset("SEM_CALL");
    EXPECT_EQ(readLines(path).size(), 3U);
    exchange(student);
    SemanticCallStats::instance().reportAndReset("SEM_CALL");
    EXPECT_EQ(readLines(path).size(), 4U);
}

TEST_F(SemanticCallStatsTest, WithoutALogPathNothingIsKeptOrWritten)
{
    SemanticCallStats::instance().setCallLogPath(std::nullopt);
    exchange(student);
    SemanticCallStats::instance().reportAndReset("SEM_CALL");
    EXPECT_FALSE(std::filesystem::exists(path));

    /// Records from before a path was set are not kept either.
    exchange(student);
    SemanticCallStats::instance().setCallLogPath(path.string());
    SemanticCallStats::instance().reportAndReset("SEM_CALL");
    EXPECT_FALSE(std::filesystem::exists(path));
}

}
}
