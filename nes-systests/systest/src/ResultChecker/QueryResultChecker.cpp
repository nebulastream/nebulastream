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

#include <cstddef>
#include <cstdint>
#include <expected>
#include <map>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Util/RowKind.hpp>
#include <fmt/format.h>

#include <Model/Verdict.hpp>
#include <ResultChecker/QueryResult.hpp>
#include <ResultChecker/ResultComparison.hpp>

namespace NES
{

namespace
{
/// Folds the changelog of an updating stream into the final table it describes. Every row starts with its row kind; rows that add
/// to the table (INSERT, UPDATE_AFTER) count +1 and rows that withdraw from it (UPDATE_BEFORE, DELETE) count -1, so the order in
/// which the rows were written does not matter.
std::expected<std::vector<std::string>, std::string> foldChangelog(const std::vector<std::string>& changelog)
{
    std::map<std::string, int64_t> counts;
    for (const auto& row : changelog)
    {
        const auto comma = row.find(',');
        const auto kind = comma == std::string::npos ? std::nullopt : parseRowKindSymbol(std::string_view{row}.substr(0, comma));
        if (not kind.has_value())
        {
            return std::unexpected(fmt::format("the changelog row '{}' does not start with a row kind (+I, -U, +U or -D)", row));
        }
        counts[row.substr(comma + 1)] += rowKindSign(*kind);
    }

    std::vector<std::string> table;
    for (const auto& [row, count] : counts)
    {
        if (count < 0)
        {
            return std::unexpected(fmt::format("the changelog withdraws the row '{}' {} more time(s) than it adds it", row, -count));
        }
        table.insert(table.end(), static_cast<size_t>(count), row);
    }
    return table;
}

bool isRowKindField(const UnqualifiedUnboundField& field)
{
    return field.getFullyQualifiedName() == QualifiedIdentifierBase<1>{rowKindIdentifier()};
}

/// The sink writes the UINT8 row kind as a symbol (+I, -U, +U, -D), so it is compared as text.
Schema<UnqualifiedUnboundField, Ordered> withRowKindAsText(const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    return schema
        | std::views::transform(
               [](const UnqualifiedUnboundField& field)
               {
                   return isRowKindField(field)
                       ? UnqualifiedUnboundField{field.getFullyQualifiedName(), DataTypeProvider::provideDataType(DataType::Type::VARSIZED)}
                       : field;
               })
        | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
}

Schema<UnqualifiedUnboundField, Ordered> withoutRowKind(const Schema<UnqualifiedUnboundField, Ordered>& schema)
{
    return schema
        | std::views::filter([](const UnqualifiedUnboundField& field) { return not isRowKindField(field); })
        | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
}
}

Verdict QueryResultCheck::check() const
{
    const auto loaded = loadQueryResult(resultFile);
    if (not loaded)
    {
        return std::unexpected(Mismatch{loaded.error()});
    }

    if (isUpdatingStream(loaded->schema) and not exactChangelog)
    {
        const auto table = foldChangelog(loaded->tuples);
        if (not table)
        {
            return std::unexpected(Mismatch{table.error()});
        }
        return toVerdict(
            compare(withoutRowKind(expectedSchema), expectedTuples, withoutRowKind(loaded->schema), *table), ComparisonOrigin::SingleQuery);
    }
    if (isUpdatingStream(loaded->schema))
    {
        return toVerdict(
            compare(withRowKindAsText(expectedSchema), expectedTuples, withRowKindAsText(loaded->schema), loaded->tuples),
            ComparisonOrigin::SingleQuery);
    }
    return toVerdict(compare(expectedSchema, expectedTuples, loaded->schema, loaded->tuples), ComparisonOrigin::SingleQuery);
}

}
