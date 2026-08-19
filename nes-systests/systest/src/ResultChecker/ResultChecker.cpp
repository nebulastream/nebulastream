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

#include <ResultChecker/ResultChecker.hpp>

#include <algorithm>
#include <expected>
#include <string>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Model/Expectation.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/ResultComparison.hpp>
#include <ResultChecker/ResultFile.hpp>

namespace NES::Systest
{

namespace
{
/// Compares rows that hold their own field names, one row per line, as text.
/// Rows arrive in no fixed order, so this sorts both sides before comparing.
Verdict compareRowsAsText(const ExpectedResult& expected, const std::vector<std::string>& actual)
{
    auto sortedExpected = expected;
    auto sortedActual = actual;
    std::ranges::sort(sortedExpected);
    std::ranges::sort(sortedActual);
    if (sortedExpected == sortedActual)
    {
        return {};
    }
    return std::unexpected(Mismatch{fmt::format(
        "Result Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n{}\n{}",
        fmt::join(sortedExpected, "\n"),
        fmt::join(sortedActual, "\n"))});
}
}

Verdict checkResult(const ResultCheck& check)
{
    const auto loaded = loadQueryResult(check.resultFile);
    if (not loaded)
    {
        return std::unexpected(Mismatch{fmt::format("result file was not written: {}", check.resultFile.string())});
    }

    /// A row that is one JSON object holds its own field names, and splitting it on commas would cut it inside the object.
    if (check.rowsAreJson)
    {
        return compareRowsAsText(check.expected, loaded->result);
    }

    /// The result file header holds the schema the engine resolved for the sink.
    /// The comparison uses it as both the expected and the actual schema.
    return toVerdict(compare(loaded->schema, check.expected, loaded->schema, loaded->result), false);
}

}
