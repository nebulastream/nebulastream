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
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RewrittenTest.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <SystestBinder.hpp>

namespace NES
{

class KeepSelectedQueriesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("KeepSelectedQueries.log", LogLevel::LOG_DEBUG); }

    static RewrittenCase queryCase(const uint64_t number, const bool runsAfterPrevious = false)
    {
        return RewrittenCase{
            .action
            = RewrittenQuery{.sql = "", .id = SystestQueryId(number), .resultFile = std::nullopt, .expectation = Expectation{ExpectedRows{}}},
            .inputFiles = {},
            .runsAfterPrevious = runsAfterPrevious};
    }

    static RewrittenTest testWith(std::vector<RewrittenCase> cases)
    {
        return RewrittenTest{.name = "test", .qualifyingPrefix = "T_", .setupStatements = {}, .cases = std::move(cases)};
    }
};

/// An empty selection selects everything, because a run without query numbers runs the whole file.
TEST_F(KeepSelectedQueriesTest, EmptySelectionKeepsEveryCase)
{
    auto runnable = testWith({queryCase(1), queryCase(2), queryCase(3)});
    Systest::keepSelectedQueries(runnable, {});
    EXPECT_EQ(runnable.cases.size(), 3U);
}

TEST_F(KeepSelectedQueriesTest, DropsTheCasesTheSelectionOmits)
{
    auto runnable = testWith({queryCase(1), queryCase(2), queryCase(3)});
    Systest::keepSelectedQueries(runnable, {SystestQueryId(2)});
    ASSERT_EQ(runnable.cases.size(), 1U);
    EXPECT_EQ(caseNumber(runnable.cases.at(0)), SystestQueryId(2));
}

/// A case that has to follow the one above it needs that one to still be there, so selecting the end of a chain keeps the whole chain.
TEST_F(KeepSelectedQueriesTest, KeepsTheChainASelectedCaseFollows)
{
    auto runnable = testWith({queryCase(1), queryCase(2, true), queryCase(3, true)});
    Systest::keepSelectedQueries(runnable, {SystestQueryId(3)});
    EXPECT_EQ(runnable.cases.size(), 3U);
}

/// A differential block is one case covering both its query numbers, so selecting either number keeps the block.
TEST_F(KeepSelectedQueriesTest, EitherNumberOfADifferentialBlockKeepsIt)
{
    const auto block = RewrittenCase{
        .action
        = RewrittenDifferential{.firstSql = "", .firstId = SystestQueryId(1), .firstResultFile = "first.csv", .secondSql = "", .secondId = SystestQueryId(2), .secondResultFile = "second.csv"},
        .inputFiles = {},
        .runsAfterPrevious = false};

    auto selectedBySecond = testWith({block});
    Systest::keepSelectedQueries(selectedBySecond, {SystestQueryId(2)});
    EXPECT_EQ(selectedBySecond.cases.size(), 1U);

    auto unselected = testWith({block});
    Systest::keepSelectedQueries(unselected, {SystestQueryId(3)});
    EXPECT_TRUE(unselected.cases.empty());
}

}
