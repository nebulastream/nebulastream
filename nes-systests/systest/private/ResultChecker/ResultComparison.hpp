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

#include <cstddef>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// Compares the expected schema and rows with the actual ones, in any row order.
/// Fields pair up by position and match by name and type.
/// Every value compares exactly except a float, which compares within a relative epsilon.
/// A mismatch holds the side by side report a developer reads when a test fails.
/// It is diffResults followed by renderReport.
[[nodiscard]] Verdict compareResults(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows);

/// A row split into its values.
/// The side records whether the test or the sink wrote the row, because the comparison treats the two differently.
template <typename Side>
struct Row
{
    std::string text;
    std::vector<std::string> values;
};

using ExpectedRow = Row<struct ExpectedSide>;
using ActualRow = Row<struct ActualSide>;

/// The two rows hold equal values.
struct Matched
{
    ExpectedRow expected;
    ActualRow actual;
};

/// No actual row holds the values of this expected row.
struct Missing
{
    ExpectedRow expected;
};

/// No expected row holds the values of this actual row.
struct Unexpected
{
    ActualRow actual;
};

/// The expected row does not hold one value per field of the expected schema, so it cannot be compared.
struct WrongFieldCount
{
    ExpectedRow expected;
    size_t fieldCount;
};

/// The two rows hold equal values in every field that both schemas share, but the schemas differ.
struct PartialMatch
{
    ExpectedRow expected;
    ActualRow actual;
};

using RowDiff = std::variant<Matched, Missing, Unexpected, WrongFieldCount, PartialMatch>;

/// Which fields of the two schemas pair up.
struct SchemaAlignment
{
    /// One entry per expected field, holding its type if the actual field at the same position matches it.
    std::vector<std::optional<DataType::Type>> matchedTypes;
    /// The positions of the actual fields that match no expected field.
    std::vector<size_t> unexpectedColumns;
};

/// What the comparison decided, before any of it is rendered as text.
struct ResultDiff
{
    Schema<UnqualifiedUnboundField, Ordered> expectedSchema;
    Schema<UnqualifiedUnboundField, Ordered> actualSchema;
    SchemaAlignment alignment;
    /// Every row of both sides in report order, so that the report shows the rows around a mismatch.
    std::vector<RowDiff> rows;

    [[nodiscard]] bool schemasMatch() const;
    [[nodiscard]] bool rowsMatch() const;
};

/// Decides which fields and rows of the two results match.
[[nodiscard]] ResultDiff diffResults(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows);

/// Renders a diff as the report that a mismatch holds.
[[nodiscard]] std::string renderReport(const ResultDiff& diff);

/// Compares two values of the given type, written as text.
/// An integer or a string compares exactly, and a float within a relative epsilon, because the text of a float is not stable.
/// NULL matches only NULL.
[[nodiscard]] bool compareValue(DataType::Type type, const std::string& expected, const std::string& actual);

}
