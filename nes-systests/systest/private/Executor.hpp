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

[[nodiscard]] RunOutcome summarize(const std::vector<ReportEntry>& entries, std::string_view appendix = {});

/// Runs one invocation of the systest target: discover -> rewrite test files -> bind -> submit -> check -> report.
class Executor
{
public:
    explicit Executor(SystestConfiguration config);
    [[nodiscard]] RunOutcome execute() const;

private:
    SystestConfiguration config;
};

}
