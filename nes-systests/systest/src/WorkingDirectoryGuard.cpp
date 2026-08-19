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

/// Empties the working directory of one systest invocation.
/// Data and result file names are derived from test file names, so they repeat across invocations.
/// For example, the result checker could read a leftover file as the result of a query that wrote nothing.
void clearWorkingDirectory(const std::filesystem::path& workingDir)
{
    std::error_code errorCode;
    for (const auto& entry : std::filesystem::directory_iterator{workingDir, errorCode})
    {
        std::filesystem::remove_all(entry.path(), errorCode);
        if (errorCode)
        {
            break;
        }
    }
    if (errorCode)
    {
        throw TestException("could not clear the working directory {}: {}", workingDir.string(), errorCode.message());
    }
}

}

WorkingDirectoryGuard::WorkingDirectoryGuard(const std::filesystem::path& directory)
{
    std::error_code errorCode;
    std::filesystem::create_directories(directory, errorCode);
    if (errorCode)
    {
        throw TestException("could not create the working directory {}: {}", directory.string(), errorCode.message());
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg) - POSIX API requires varargs
    lockedDirectory.descriptor = ::open(directory.c_str(), O_DIRECTORY | O_RDONLY | O_CLOEXEC);
    if (lockedDirectory.descriptor < 0)
    {
        throw TestException("could not open the working directory {}: {}", directory.string(), getErrorMessageFromERRNO());
    }
    /// The kernel releases a flock however the process ends, so a crashed run leaves no lock to remove by hand.
    /// An flock also works across containers.
    if (::flock(lockedDirectory.descriptor, LOCK_EX | LOCK_NB) != 0)
    {
        throw TestException(
            "another run holds the working directory {}, and each would clear it under the other. "
            "Wait for that run, or pass --workingDir to use a different working directory",
            directory.string());
    }
    clearWorkingDirectory(directory);
}

WorkingDirectoryGuard::LockedDirectory::~LockedDirectory()
{
    if (descriptor >= 0)
    {
        ::close(descriptor);
    }
}

}
