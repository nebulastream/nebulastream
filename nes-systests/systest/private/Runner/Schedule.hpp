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

#include <cstddef>
#include <functional>
#include <vector>

#include <Model/RunnableTest.hpp>

namespace NES
{

/// One case of one test file, and where it belongs in the run.
/// The position is the slot its check takes in the report, so the report keeps test file order however the cases interleave.
struct Job
{
    size_t runnable = 0;
    size_t index = 0;
    size_t position = 0;

    bool operator==(const Job& other) const = default;
};

/// Decides which cases the runner may submit next.
/// A test file hands out its cases in order, and one that has to follow the case before it holds the file back until
/// that case is terminal.
/// Every dependency points at the case immediately above, so a cursor and a completion mark per file are enough.
class Schedule
{
public:
    explicit Schedule(const std::vector<std::reference_wrapper<const RunnableTest>>& tests);

    /// How many cases the whole run holds, which is how many checks it produces.
    [[nodiscard]] size_t size() const { return total; }

    /// The jobs the runner may submit before anything has finished.
    [[nodiscard]] std::vector<Job> ready();

    /// Records a finished job and returns the jobs its completion released.
    [[nodiscard]] std::vector<Job> completed(const Job& job);

private:
    /// How far one test file has got: which of its cases have gone out, and which have come back.
    class File
    {
    public:
        File(size_t runnable, size_t offset, const RunnableTest& test);

        [[nodiscard]] size_t caseCount() const { return test.get().cases.size(); }

        void markDone(const size_t index) { done.at(index) = true; }

        /// Hands out the cases this file may now submit, stopping at the first one still blocked.
        [[nodiscard]] std::vector<Job> release();

    private:
        [[nodiscard]] bool blocked(size_t index) const;

        std::reference_wrapper<const RunnableTest> test;
        size_t runnable;
        size_t offset;
        size_t cursor = 0;
        std::vector<bool> done;
    };

    std::vector<File> files;
    size_t total = 0;
};

}
