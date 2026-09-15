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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

namespace
{

RewrittenCase queryCase(const uint64_t number)
{
    return RewrittenCase{
        .action = RewrittenQuery{
            .sql = "",
            .id = SystestQueryId{number},
            .resultFile = std::nullopt,
            .inputFiles = {},
            .expectation = Expectation{ExpectedRows{}}}};
}

RunnableTestFile fileWith(std::vector<RewrittenCase> cases)
{
    return RunnableTestFile{.name = "test", .key = "T", .qualifyingPrefix = "T_", .setupStatements = {}, .cases = std::move(cases)};
}

}

class KeepSelectedCasesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("KeepSelectedCases.log", LogLevel::LOG_DEBUG); }
};

/// A run without query numbers runs the whole file, so the empty selection has to keep everything rather than nothing.
TEST_F(KeepSelectedCasesTest, EmptySelectionKeepsEveryCase)
{
    auto file = fileWith({queryCase(1), queryCase(2), queryCase(3)});
    keepSelectedCases(file, {});
    EXPECT_EQ(file.cases.size(), 3U);
}

TEST_F(KeepSelectedCasesTest, DropsTheCasesTheSelectionOmits)
{
    auto file = fileWith({queryCase(1), queryCase(2), queryCase(3)});
    keepSelectedCases(file, {SystestQueryId{2}});
    ASSERT_EQ(file.cases.size(), 1U);
    EXPECT_EQ(caseNumber(file.cases.at(0)), SystestQueryId{2});
}

/// A differential block is one case covering both its query numbers, so selecting either number keeps the block.
TEST_F(KeepSelectedCasesTest, EitherNumberOfADifferentialBlockKeepsIt)
{
    const auto block = RewrittenCase{
        .action = RewrittenDifferential{
            .firstSql = "",
            .firstId = SystestQueryId{1},
            .firstResultFile = "first.csv",
            .secondSql = "",
            .secondId = SystestQueryId{2},
            .secondResultFile = "second.csv"}};

    auto selectedBySecond = fileWith({block});
    keepSelectedCases(selectedBySecond, {SystestQueryId{2}});
    EXPECT_EQ(selectedBySecond.cases.size(), 1U);

    auto unselected = fileWith({block});
    keepSelectedCases(unselected, {SystestQueryId{3}});
    EXPECT_TRUE(unselected.cases.empty());
}

}
