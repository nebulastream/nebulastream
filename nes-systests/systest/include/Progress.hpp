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

#include <chrono>
#include <cstddef>

#include <Model/RunnableTest.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>

namespace NES
{

/// Prints one line per finished case, so a long run reports progress instead of printing nothing until it ends.
/// Printing a total requires every test file to be prepared before the first case runs.
/// Each line shows the test file it belongs to, because cases from several files finish interleaved.
/// Not thread safe, because the runner checks and reports one case at a time from a single thread.
class Progress
{
public:
    explicit Progress(size_t totalCases);

    /// Prints how many cases and test files the run holds.
    void beginRun(size_t files) const;

    void report(const TestCaseId& id, const RunnableCase& testCase, const Verdict& verdict, std::chrono::steady_clock::duration elapsed);

private:
    size_t done = 0;
    size_t total;
};

}
