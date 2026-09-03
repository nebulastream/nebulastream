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

#pragma once

#include <filesystem>

namespace NES
{

/// RAII guard that owns the working directory for one systest invocation and empties it on construction.
/// This guarantees that no concurrent runs make conflicting updates or remove result files that another run reads.
/// Implemented as a lock on the directory that makes a second run report the conflict.
class WorkingDirectoryGuard
{
public:
    /// Constructing acquires the lock, then empties the directory, and throws on error.
    explicit WorkingDirectoryGuard(const std::filesystem::path& directory);

    WorkingDirectoryGuard(const WorkingDirectoryGuard&) = delete;
    WorkingDirectoryGuard& operator=(const WorkingDirectoryGuard&) = delete;
    WorkingDirectoryGuard(WorkingDirectoryGuard&&) = delete;
    WorkingDirectoryGuard& operator=(WorkingDirectoryGuard&&) = delete;

    ~WorkingDirectoryGuard() = default;

private:
    /// Owns the descriptor of the locked directory. Closing it releases the lock, which happens when the guard goes out
    /// of scope. It is also closed when the constructor throws after opening it, which a bare descriptor would not,
    /// because a constructor that throws runs no destructor.
    struct LockedDirectory
    {
        LockedDirectory() = default;
        LockedDirectory(const LockedDirectory&) = delete;
        LockedDirectory& operator=(const LockedDirectory&) = delete;
        LockedDirectory(LockedDirectory&&) = delete;
        LockedDirectory& operator=(LockedDirectory&&) = delete;
        ~LockedDirectory();

        int descriptor = -1;
    } lockedDirectory;
};

}
