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

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <system_error>

#include <unistd.h>

namespace NES::Testing
{

/// A fresh directory under the system temp directory for one test, removed when the test is done.
/// The name holds the process id, the time and a counter, so tests in one process and in processes that ctest runs side
/// by side never share a directory.
class TemporaryDirectory
{
public:
    TemporaryDirectory()
    {
        static std::atomic_uint64_t counter = 0;
        const auto uniqueId = std::chrono::steady_clock::now().time_since_epoch().count();
        path = std::filesystem::temp_directory_path()
            / ("nes-systest-" + std::to_string(::getpid()) + "-" + std::to_string(uniqueId) + "-" + std::to_string(counter.fetch_add(1)));
        std::filesystem::create_directories(path);
    }

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
