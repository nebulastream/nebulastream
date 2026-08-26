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

#include <cstddef>
#include <functional>
#include <optional>
#include <vector>

#include <gtest/gtest.h>

#include <Model/Expectation.hpp>
#include <Model/RewrittenTest.hpp>
#include <Model/SystestQueryId.hpp>
#include <Runner/Schedule.hpp>

namespace NES
{
namespace
{

/// A test file with the given cases, where a true entry marks a case that has to follow the one above it.
RewrittenTest fileOf(const std::vector<bool>& runsAfterPrevious)
{
    RewrittenTest test;
    for (const bool waits : runsAfterPrevious)
    {
        test.cases.push_back(RewrittenCase{
            .action
            = RewrittenQuery{.sql = "SELECT 1", .id = SystestQueryId{1}, .resultFile = std::nullopt, .inputFiles = {}, .expectation = ExpectedRows{}},
            .runsAfterPrevious = waits});
    }
    return test;
}

std::vector<std::reference_wrapper<const RewrittenTest>> allOf(const std::vector<RewrittenTest>& tests)
{
    return {tests.begin(), tests.end()};
}

/// Returns the index each job has within its own test file, which the ordering rules act on.
std::vector<size_t> indices(const std::vector<Job>& jobs)
{
    std::vector<size_t> result;
    result.reserve(jobs.size());
    for (const auto& job : jobs)
    {
        result.push_back(job.index);
    }
    return result;
}

}

/// A file whose queries wait for nothing hands out all of them at once.
TEST(ScheduleTest, ReleasesEveryIndependentQueryAtOnce)
{
    const std::vector tests{fileOf({false, false, false})};
    Schedule schedule{allOf(tests)};

    EXPECT_EQ(schedule.size(), 3U);
    EXPECT_EQ(indices(schedule.ready()), (std::vector<size_t>{0, 1, 2}));
}

/// A file that runs in order hands out one query at a time, and the next only once the one before it has finished.
TEST(ScheduleTest, HandsOutASequentialFileOneQueryAtATime)
{
    const std::vector tests{fileOf({true, true, true})};
    Schedule schedule{allOf(tests)};

    const auto first = schedule.ready();
    ASSERT_EQ(indices(first), (std::vector<size_t>{0}));
    EXPECT_EQ(indices(schedule.completed(first.at(0))), (std::vector<size_t>{1}));
}

/// A query that waits holds back only the queries below it, and the ones above it went out already.
TEST(ScheduleTest, StopsAtTheFirstQueryThatWaits)
{
    const std::vector tests{fileOf({false, false, true, false})};
    Schedule schedule{allOf(tests)};

    const auto first = schedule.ready();
    ASSERT_EQ(indices(first), (std::vector<size_t>{0, 1}));

    /// The query that waits needs the one directly above it, so finishing the first one releases nothing.
    EXPECT_TRUE(schedule.completed(first.at(0)).empty());
    EXPECT_EQ(indices(schedule.completed(first.at(1))), (std::vector<size_t>{2, 3}));
}

/// One file waiting does not hold up another, and every query keeps the place its check takes in the report.
TEST(ScheduleTest, RunsFilesIndependentlyAndNumbersTheReportInFileOrder)
{
    const std::vector tests{fileOf({true, true}), fileOf({false, false})};
    Schedule schedule{allOf(tests)};

    EXPECT_EQ(schedule.size(), 4U);
    const auto first = schedule.ready();
    ASSERT_EQ(first.size(), 3U);
    EXPECT_EQ(first.at(0).position, 0U);
    EXPECT_EQ(first.at(1).position, 2U);
    EXPECT_EQ(first.at(2).position, 3U);

    const auto released = schedule.completed(first.at(0));
    ASSERT_EQ(released.size(), 1U);
    EXPECT_EQ(released.at(0).position, 1U);
}

}
