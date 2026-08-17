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

#include <WorkingDirectoryGuard.hpp>

#include <filesystem>
#include <system_error>

#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>

#include <Util/Files.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Empties the working directory used for one systest invocation.
/// Data and result file names come from the test file and the query number, so they repeat across invocations.
/// The checker would read a leftover file as the result of a query that wrote nothing.
/// A filesystem error throws, so the run ends rather than the process.
void clearWorkingDirectory(const std::filesystem::path& workingDir)
{
    std::error_code errorCode;
    std::filesystem::remove_all(workingDir, errorCode);
    if (errorCode)
    {
        throw TestException("could not clear the working directory {}: {}", workingDir.string(), errorCode.message());
    }

    std::filesystem::create_directories(workingDir, errorCode);
    if (errorCode)
    {
        throw TestException("could not create the working directory {}: {}", workingDir.string(), errorCode.message());
    }
}

/// Returns the path of the lock file for a working directory.
/// The lock file is a sibling of the directory, because emptying the directory would delete a lock file inside it.
std::filesystem::path lockFileFor(const std::filesystem::path& workingDir)
{
    return workingDir.string() + ".lock";
}

}

WorkingDirectoryGuard::WorkingDirectoryGuard(const std::filesystem::path& directory)
{
    const auto lockFile = lockFileFor(directory);
    std::error_code errorCode;
    std::filesystem::create_directories(lockFile.parent_path(), errorCode);

    claim = ::open(lockFile.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
    if (claim < 0)
    {
        throw TestException("could not open the working directory lock {}: {}", lockFile.string(), getErrorMessageFromERRNO());
    }
    /// The kernel releases a flock however the process ends, so a crashed run leaves no lock to remove by hand.
    /// A flock also works across containers, and each run is its own container.
    if (::flock(claim, LOCK_EX | LOCK_NB) != 0)
    {
        ::close(claim);
        throw TestException(
            "another run holds the working directory {}, and each would clear it under the other. "
            "Wait for that run, or pass --workingDir to use a different directory",
            directory.string());
    }
    clearWorkingDirectory(directory);
}

WorkingDirectoryGuard::~WorkingDirectoryGuard()
{
    ::close(claim);
}

}
