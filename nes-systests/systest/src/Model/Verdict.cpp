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

#include <Model/Verdict.hpp>

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

bool hasPassed(const CaseOutcome& outcome)
{
    return std::holds_alternative<Verdict>(outcome) and std::get<Verdict>(outcome).has_value();
}

/// @param originFile the .test file that failed
/// @param override the override that failed (could be empty)
/// @param context the setup stage (parsing, reading the file, rewriting, etc.) where the failure occurred
/// @param exception the specific failure that occurred, std::exception by choice (captures filesystem errors, etc.)
ReportEntry createFailedFileEntry(
    std::string originFile, ConfigurationOverride override, const std::string_view context, const std::exception& exception)
{
    const std::string_view message{exception.what()};
    const auto* nesException = dynamic_cast<const Exception*>(&exception);
    const auto code = nesException != nullptr ? nesException->code() : ErrorCode::UnknownException;
    return ReportEntry{
        .id = TestCaseId{.originFile = std::move(originFile), .queryIdInFile = std::nullopt, .overrides = std::move(override)},
        .outcome = Verdict{
            std::unexpected{Mismatch{fmt::format("{}: {}", context, message.empty() ? fmt::format("{}", code) : std::string{message})}}}};
}

}
