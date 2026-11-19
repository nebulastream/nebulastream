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
    const auto first = loadQueryResult(firstResultFile, firstHasHeader);
    if (not first)
    {
        return std::unexpected(Mismatch{fmt::format("the first query of the differential block: {}", first.error())});
    }
    const auto second = loadQueryResult(secondResultFile, secondHasHeader);
    if (not second)
    {
        return std::unexpected(Mismatch{fmt::format("the second query of the differential block: {}", second.error())});
    }

    /// A side without a header borrows the schema of the other side, but at least one side must provide one to compare rows field by field.
    if (not first->schema.has_value() and not second->schema.has_value())
    {
        return std::unexpected(
            Mismatch{"a differential block needs a schema header on at least one side, but neither result file has one"});
    }
    const auto& firstSchema = first->schema.has_value() ? *first->schema : *second->schema;
    const auto& secondSchema = second->schema.has_value() ? *second->schema : *first->schema;

    return toVerdict(compare(firstSchema, first->tuples, secondSchema, second->tuples), ComparisonOrigin::DifferentialBlock);
}

}
