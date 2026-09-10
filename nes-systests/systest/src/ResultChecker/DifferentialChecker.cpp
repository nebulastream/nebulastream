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

#include <ResultChecker/DifferentialChecker.hpp>

#include <expected>
#include <ranges>
#include <string>

#include <fmt/format.h>

#include <Model/Verdict.hpp>
#include <ResultChecker/ResultComparison.hpp>
#include <ResultChecker/ResultFile.hpp>

namespace NES::Systest
{

Verdict checkDifferential(const DifferentialCheck& check)
{
    const auto fail = [](const std::string& reason) -> Verdict
    { return std::unexpected(Mismatch{reason + "\n\nThis error happened during differential query execution."}); };

    const auto result1 = loadQueryResult(check.resultFile);
    const auto result2 = loadQueryResult(check.differentialResultFile);
    if (not result1)
    {
        return fail(fmt::format("Failed to load first result file for differential query comparison: {}", check.resultFile.string()));
    }
    if (not result2)
    {
        return fail(
            fmt::format("Failed to load second result file for differential query comparison: {}", check.differentialResultFile.string()));
    }
    if (std::ranges::size(result1->schema) == 0)
    {
        return fail(fmt::format("First result file is empty or has no schema: {}", check.resultFile.string()));
    }
    if (std::ranges::size(result2->schema) == 0)
    {
        return fail(fmt::format("Second result file is empty or has no schema: {}", check.differentialResultFile.string()));
    }

    return toVerdict(compare(result1->schema, result1->result, result2->schema, result2->result), true);
}

}
