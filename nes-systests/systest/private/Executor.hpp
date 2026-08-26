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

#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <Config/Config.hpp>
#include <Model/Verdict.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Two types, not one with an optional code, so a caller cannot read an absent code.
struct RunSucceeded
{
    std::string report;
};

struct RunFailed
{
    std::string report;
    ErrorCode errorCode;
};

using RunOutcome = std::variant<RunSucceeded, RunFailed>;

/// Tallies entries into the run's outcome: one line per failure and per skipped partition.
[[nodiscard]] RunOutcome summarize(const std::vector<ReportEntry>& entries, std::string_view appendix = {});

/// Runs one invocation: discover, rewrite every file up front, set up, submit, check, report.
class Executor
{
public:
    explicit Executor(SystestConfiguration config);
    [[nodiscard]] RunOutcome execute();

private:
    SystestConfiguration config;
};

}
