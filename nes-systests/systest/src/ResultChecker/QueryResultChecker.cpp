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

#include <ResultChecker/QueryResultChecker.hpp>

#include <expected>

#include <Model/Verdict.hpp>
#include <ResultChecker/QueryResult.hpp>
#include <ResultChecker/ResultComparison.hpp>

namespace NES
{

Verdict QueryResultCheck::check() const
{
    const auto loaded = loadQueryResult(resultFile, hasHeader);
    if (not loaded)
    {
        return std::unexpected(Mismatch{loaded.error()});
    }

    /// A result without a header has no schema to check, so its rows are compared against the declared schema alone.
    return toVerdict(
        compare(expectedSchema, expectedTuples, loaded->schema.value_or(expectedSchema), loaded->tuples), ComparisonOrigin::SingleQuery);
}

}
