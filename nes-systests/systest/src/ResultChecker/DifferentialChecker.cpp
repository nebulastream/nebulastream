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

#include <fmt/format.h>

#include <Model/Verdict.hpp>
#include <ResultChecker/QueryResult.hpp>
#include <ResultChecker/ResultComparison.hpp>

namespace NES
{

Verdict DifferentialCheck::check() const
{
    const auto firstRows = loadQueryResult(first.resultFile, first.expectedHeader);
    if (not firstRows)
    {
        return std::unexpected(Mismatch{fmt::format("the first query of the differential block: {}", firstRows.error())});
    }
    const auto secondRows = loadQueryResult(second.resultFile, second.expectedHeader);
    if (not secondRows)
    {
        return std::unexpected(Mismatch{fmt::format("the second query of the differential block: {}", secondRows.error())});
    }

    /// Each side's rows are read with the schema of its own bound plan, so two output formats without a header compare as well.
    return toVerdict(compare(first.schema, *firstRows, second.schema, *secondRows), ComparisonOrigin::DifferentialBlock);
}

}
