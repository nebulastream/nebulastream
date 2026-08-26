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
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <fmt/format.h>

#include <Model/ConfigurationOverride.hpp>
#include <Model/TestCaseId.hpp>
#include <ErrorHandling.hpp>

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

[[nodiscard]] inline bool hasPassed(const CaseOutcome& outcome)
{
    return std::holds_alternative<Verdict>(outcome) and std::get<Verdict>(outcome).has_value();
}

struct StatementTiming
{
    /// Worker-recorded time from running to stopped.
    std::chrono::milliseconds execution{};
};

/// One report line, for a test case or for a whole file that failed.
struct ReportEntry
{
    TestCaseId id;
    CaseOutcome outcome;
};

/// A report entry for a file that failed before it had test cases.
/// An empty message falls back to the error code, so the reason is never blank.
[[nodiscard]] inline ReportEntry createFailedFileEntry(
    std::string originFile, ConfigurationOverride overrides, const std::string_view activity, const std::exception& exception)
{
    const std::string_view message{exception.what()};
    const auto* nesException = dynamic_cast<const Exception*>(&exception);
    const auto code = nesException != nullptr ? nesException->code() : ErrorCode::UnknownException;
    return ReportEntry{
        .id = TestCaseId{.originFile = std::move(originFile), .queryIdInFile = std::nullopt, .overrides = std::move(overrides)},
        .outcome = Verdict{
            std::unexpected{Mismatch{fmt::format("{}: {}", activity, message.empty() ? fmt::format("{}", code) : std::string{message})}}}};
}

}
