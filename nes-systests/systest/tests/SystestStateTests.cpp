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

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <system_error>

#include <Model/SystestQueryId.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SystestState.hpp>

namespace
{
std::filesystem::path createUniqueTempDirectory()
{
    static std::atomic_uint64_t counter = 0;
    const auto uniqueId = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path()
        / ("nes-systest-state-" + std::to_string(uniqueId) + "-" + std::to_string(counter.fetch_add(1)));
    std::filesystem::create_directories(path);
    return path;
}

class TemporaryDirectory
{
public:
    TemporaryDirectory() : path(createUniqueTempDirectory()) { }

    ~TemporaryDirectory()
    {
        std::error_code errorCode;
        std::filesystem::remove_all(path, errorCode);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;
    TemporaryDirectory(TemporaryDirectory&&) = delete;
    TemporaryDirectory& operator=(TemporaryDirectory&&) = delete;

    [[nodiscard]] const std::filesystem::path& get() const { return path; }

private:
    std::filesystem::path path;
};
}

namespace NES::Systest
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
    const TemporaryDirectory tempDir;

    const auto resultFile = SystestQuery::resultFile(tempDir.get(), "left/same", SystestQueryId(1));

    EXPECT_EQ(resultFile, tempDir.get() / "results" / "left" / "same_1.csv");
    EXPECT_TRUE(std::filesystem::is_directory(resultFile.parent_path()));
}
}
