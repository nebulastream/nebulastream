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

#include <cstdint>
#include <string>

namespace NES
{

struct ReplayOperatorStatistics
{
    std::string nodeFingerprint;
    uint64_t inputTuples = 0;
    uint64_t outputTuples = 0;
    uint64_t taskCount = 0;
    uint64_t executionTimeNanos = 0;

    [[nodiscard]] double averageExecutionTimeMs() const
    {
        return taskCount == 0 ? 0.0 : (static_cast<double>(executionTimeNanos) / 1'000'000.0) / static_cast<double>(taskCount);
    }

    [[nodiscard]] double averageInputTuples() const
    {
        return taskCount == 0 ? 0.0 : static_cast<double>(inputTuples) / static_cast<double>(taskCount);
    }

    [[nodiscard]] double averageOutputTuples() const
    {
        return taskCount == 0 ? 0.0 : static_cast<double>(outputTuples) / static_cast<double>(taskCount);
    }

    [[nodiscard]] double selectivity() const
    {
        return inputTuples == 0 ? 1.0 : static_cast<double>(outputTuples) / static_cast<double>(inputTuples);
    }

    [[nodiscard]] bool operator==(const ReplayOperatorStatistics& other) const = default;
};

}
