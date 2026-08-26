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
#include <expected>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <Executor.hpp>

namespace NES
{

namespace
{

ReportEntry createEntry(const uint64_t number, CaseOutcome outcome)
{
    return ReportEntry{
        .id = TestCaseId{.originFile = "f", .queryIdInFile = SystestQueryId{number}, .overrides = {}}, .outcome = std::move(outcome)};
}

ReportEntry createPassedEntry(const uint64_t number)
{
    return createEntry(number, Verdict{Success{}});
}

ReportEntry createSkippedEntry(const uint64_t number, std::string reason)
{
    return createEntry(number, Skipped{.reason = std::move(reason)});
}

}

class SummarizeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SummarizeTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(SummarizeTest, AnEmptyRunFails)
{
    const auto outcome = summarize({});
    const auto* failed = std::get_if<RunFailed>(&outcome);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("no query ran")) << failed->report;
}

TEST_F(SummarizeTest, GroupsSkipsPerPartitionAndReasonAndStillSucceeds)
{
    const auto outcome = summarize({createSkippedEntry(1, "r"), createSkippedEntry(2, "r")});
    const auto* succeeded = std::get_if<RunSucceeded>(&outcome);
    ASSERT_NE(succeeded, nullptr);
    EXPECT_EQ(succeeded->report, "0 queries passed, 0 failed, 2 skipped\n  SKIP  f: 2 test cases: r\n");
}

TEST_F(SummarizeTest, AFailedFileCountsAsOneFailure)
{
    const auto outcome = summarize({createFailedFileEntry("g", {}, "could not prepare", TestException("boom")), createPassedEntry(1)});
    const auto* failed = std::get_if<RunFailed>(&outcome);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("1 queries passed, 1 failed\n  FAIL  g: could not prepare: ")) << failed->report;
}

TEST_F(SummarizeTest, PutsTheAppendixAfterTheDetails)
{
    const auto outcome = summarize({createPassedEntry(1), createEntry(2, Verdict{std::unexpected{Mismatch{"differs"}}})}, "appendix\n");
    const auto* failed = std::get_if<RunFailed>(&outcome);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->report, "1 queries passed, 1 failed\n  FAIL  f:2: differs\nappendix\n");
}

/// Sorted, so reports diff cleanly across runs.
TEST_F(SummarizeTest, SeparatesSkipsByOverridesAndByReason)
{
    auto underOverride = createSkippedEntry(3, "r");
    underOverride.id.overrides = ConfigurationOverride{{"k", "v"}};
    const auto outcome = summarize({createSkippedEntry(1, "r"), createSkippedEntry(2, "s"), underOverride});
    const auto* succeeded = std::get_if<RunSucceeded>(&outcome);
    ASSERT_NE(succeeded, nullptr);
    EXPECT_EQ(
        succeeded->report,
        "0 queries passed, 0 failed, 3 skipped\n"
        "  SKIP  f: 1 test cases: r\n"
        "  SKIP  f: 1 test cases: s\n"
        "  SKIP  f [k=v]: 1 test cases: r\n");
}

TEST_F(SummarizeTest, ListsFailuresBeforeSkipsAndCountsSkipsApart)
{
    const auto outcome
        = summarize({createSkippedEntry(1, "r"), createEntry(2, Verdict{std::unexpected{Mismatch{"differs"}}}), createPassedEntry(3)});
    const auto* failed = std::get_if<RunFailed>(&outcome);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(failed->report, "1 queries passed, 1 failed, 1 skipped\n  FAIL  f:2: differs\n  SKIP  f: 1 test cases: r\n");
    EXPECT_EQ(failed->errorCode, ErrorCode::TestException);
}

TEST_F(SummarizeTest, KeepsTheAppendixWhenTheRunPassesOrRanNothing)
{
    const auto passing = summarize({createPassedEntry(1)}, "appendix\n");
    const auto* succeeded = std::get_if<RunSucceeded>(&passing);
    ASSERT_NE(succeeded, nullptr);
    EXPECT_EQ(succeeded->report, "1 queries passed, 0 failed\nappendix\n");

    const auto empty = summarize({}, "appendix\n");
    const auto* failed = std::get_if<RunFailed>(&empty);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.ends_with("\nappendix\n")) << failed->report;
}

}
