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

#include <filesystem>
#include <string>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SystestState.hpp>
#include <TemporaryDirectory.hpp>

namespace NES
{
class SystestStateTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestStateTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestStateTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestStateTest test class."); }
};

TEST_F(SystestStateTest, ResultFilesCreateDirectoriesForNestedTestNames)
{
    const Testing::TemporaryDirectory tempDir;

    const auto resultFile = SystestQuery::resultFile(tempDir.get(), "left/same", SystestQueryId(1));

    EXPECT_EQ(resultFile, tempDir.get() / "results" / "left" / "same_1.csv");
    EXPECT_TRUE(std::filesystem::is_directory(resultFile.parent_path()));
}

/// A query with several sinks writes one result file per sink, and the files of further sinks sit next to the first one,
/// also when the test name holds a directory.
TEST_F(SystestStateTest, ResultFilesOfFurtherSinksSitNextToTheFirst)
{
    const Testing::TemporaryDirectory tempDir;

    const auto firstSink = SystestQuery::resultFile(tempDir.get(), "left/same", SystestQueryId(1), 0);
    const auto secondSink = SystestQuery::resultFile(tempDir.get(), "left/same", SystestQueryId(1), 1);

    EXPECT_EQ(secondSink, tempDir.get() / "results" / "left" / "same_1_sink1.csv");
    EXPECT_EQ(secondSink.parent_path(), firstSink.parent_path());
}
}
