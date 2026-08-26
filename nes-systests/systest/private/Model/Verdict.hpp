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
#include <exception>
#include <expected>
#include <string>
#include <string_view>
#include <variant>

#include <Model/ConfigurationOverride.hpp>
#include <Model/TestCaseId.hpp>

namespace NES
{

/// Why a check failed, as text describing how the actual output differs from the expected one.
/// A struct, so it can later store more structured mismatch information without touching call sites.
struct Mismatch
{
    std::string detail;
};

struct Success
{
};

using Verdict = std::expected<Success, Mismatch>;

struct Skipped
{
    std::string reason;
};

using CaseOutcome = std::variant<Verdict, Skipped>;

[[nodiscard]] bool hasPassed(const CaseOutcome& outcome);

struct StatementTiming
{
    /// Worker-recorded time from running to stopped.
    std::chrono::nanoseconds execution{};
};

/// One report line, for a test case or for a whole file that failed.
struct ReportEntry
{
    TestCaseId id;
    CaseOutcome outcome;
};

/// A report entry for a file that failed before it planned or ran any actual test cases.
/// For example, a query that failed to parse or a setup statement that threw.
[[nodiscard]] ReportEntry
createFailedFileEntry(std::string originFile, ConfigurationOverride override, std::string_view context, const std::exception& exception);

}
