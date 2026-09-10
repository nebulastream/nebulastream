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

/// Owns the working directory for one systest invocation and empties it on construction.
/// Every invocation defaults to the same directory, so a second run would delete the data and result files the first one still reads.
/// The lock makes the second run report the conflict instead.
class WorkingDirectoryGuard
{
public:
    explicit WorkingDirectoryGuard(const std::filesystem::path& directory);

    WorkingDirectoryGuard(const WorkingDirectoryGuard&) = delete;
    WorkingDirectoryGuard& operator=(const WorkingDirectoryGuard&) = delete;
    WorkingDirectoryGuard(WorkingDirectoryGuard&&) = delete;
    WorkingDirectoryGuard& operator=(WorkingDirectoryGuard&&) = delete;

    /// Closing the lock file releases the lock.
    ~WorkingDirectoryGuard();

private:
    int claim = -1;
};

}
