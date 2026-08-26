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

ReportEntry createFailedFileEntry(
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
