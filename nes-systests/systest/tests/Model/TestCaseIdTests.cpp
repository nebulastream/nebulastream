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

/// Compiles the model headers with nothing included before them, so a header that is missing an include fails to build here.

#include <string>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/TestCaseId.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

class TestCaseIdTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TestCaseIdTest.log", LogLevel::LOG_DEBUG); }
};

/// The number is printed as it is, so a file with more than 99 queries reports like any other.
TEST_F(TestCaseIdTest, PrintsFileAndQueryNumber)
{
    EXPECT_EQ(
        fmt::format("{}", TestCaseId{.originFile = "operator/join/JoinNull", .queryIdInFile = SystestQueryId(7), .overrides = {}}),
        "operator/join/JoinNull:7");
    EXPECT_EQ(
        fmt::format("{}", TestCaseId{.originFile = "large/Many", .queryIdInFile = SystestQueryId(123), .overrides = {}}), "large/Many:123");
    EXPECT_EQ(
        fmt::format("{}", TestCaseId{.originFile = "bug/NoQuery", .queryIdInFile = INVALID<SystestQueryId>, .overrides = {}}),
        "bug/NoQuery");
}

/// A query with configuration alternatives runs once per alternative, so the overrides are part of what tells the runs apart.
TEST_F(TestCaseIdTest, PrintsTheOverridesOfTheRun)
{
    ConfigurationOverride overrides;
    overrides["worker.query_engine.number_of_worker_threads"] = "2";

    EXPECT_EQ(
        fmt::format("{}", TestCaseId{.originFile = "operator/join/JoinNull", .queryIdInFile = SystestQueryId(1), .overrides = overrides}),
        "operator/join/JoinNull:1 [worker.query_engine.number_of_worker_threads=2]");
}

}
