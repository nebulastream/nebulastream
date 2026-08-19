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
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include <gtest/gtest.h>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <TemporaryDirectory.hpp>
#include <WorkingDirectoryGuard.hpp>

namespace
{

/// The number of descriptors this process holds, so a test can show that a failed guard left none behind.
std::size_t openDescriptors()
{
    return static_cast<std::size_t>(
        std::distance(std::filesystem::directory_iterator{"/proc/self/fd"}, std::filesystem::directory_iterator{}));
}

}

namespace NES
{

class WorkingDirectoryGuardTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("WorkingDirectoryGuardTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(WorkingDirectoryGuardTest, EmptiesTheDirectoryAndLeavesNoArtifacts)
{
    const Testing::TemporaryDirectory tempDir;
    const auto workingDir = tempDir.get() / "work";
    std::filesystem::create_directories(workingDir / "stale");
    std::ofstream{workingDir / "stale.csv"};

    const WorkingDirectoryGuard guard{workingDir.string() + "/"};

    EXPECT_TRUE(std::filesystem::is_directory(workingDir));
    EXPECT_TRUE(std::filesystem::is_empty(workingDir));
    EXPECT_EQ(std::distance(std::filesystem::directory_iterator{tempDir.get()}, std::filesystem::directory_iterator{}), 1);
}

/// Two runs on one directory would each clear it under the other, so the second one is refused. The directory is the
/// same with or without the trailing separator.
TEST_F(WorkingDirectoryGuardTest, RefusesASecondGuardOnTheSameDirectory)
{
    const Testing::TemporaryDirectory tempDir;
    const auto workingDir = tempDir.get() / "work";

    const WorkingDirectoryGuard guard{workingDir};

    EXPECT_THROW(WorkingDirectoryGuard{workingDir}, Exception);
    EXPECT_THROW(WorkingDirectoryGuard{workingDir.string() + "/"}, Exception);
}

TEST_F(WorkingDirectoryGuardTest, ReleasesTheDirectoryWhenTheGuardGoesOutOfScope)
{
    const Testing::TemporaryDirectory tempDir;
    const auto workingDir = tempDir.get() / "work";

    {
        const WorkingDirectoryGuard guard{workingDir};
    }

    EXPECT_NO_THROW(WorkingDirectoryGuard{workingDir});
}

/// A guard that fails to take the lock has opened the directory already, and a constructor that throws runs no destructor,
/// so the descriptor has to be owned by a member to be closed.
TEST_F(WorkingDirectoryGuardTest, ClosesTheDirectoryWhenTheLockIsTaken)
{
    const Testing::TemporaryDirectory tempDir;
    const auto workingDir = tempDir.get() / "work";
    const WorkingDirectoryGuard guard{workingDir};

    const auto before = openDescriptors();
    EXPECT_THROW(WorkingDirectoryGuard{workingDir}, Exception);

    EXPECT_EQ(openDescriptors(), before);
}

}
