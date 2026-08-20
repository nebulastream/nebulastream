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

#include <ResultChecker/ResultComparison.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iterator>
#include <limits>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nameof.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
/// Compares two field values as text of the given type.
/// An integer or a string compares exactly, and a float within a relative epsilon, because the text of a float is not stable.
/// NULL matches only NULL.
template <typename T>
bool compareStringAsTypeWithError(const std::string& left, const std::string& right)
{
    if (toLowerCase(left) == "null" and toLowerCase(right) == "null")
    {
        return true;
    }
    if ((toLowerCase(left) == "null" and toLowerCase(right) != "null") or (toLowerCase(left) != "null" and toLowerCase(right) == "null"))
    {
        return false;
    }

    static constexpr auto EPSILON = 1e-5;
    if constexpr (std::is_floating_point_v<T>)
    {
        const auto doubleLeft = std::stod(left);
        const auto doubleRight = std::stod(right);
        const auto absDoubleLeft = std::abs(doubleLeft);
        const auto absDoubleRight = std::abs(doubleRight);
        const auto absDiff = std::abs(doubleLeft - doubleRight);
        if (doubleLeft == doubleRight)
        {
            return true;
        }
        if (doubleLeft == 0.0 || doubleRight == 0.0 || (absDoubleLeft + absDoubleRight < std::numeric_limits<double>::min()))
        {
            return absDiff < (EPSILON * std::numeric_limits<double>::min());
        }
        const auto relativeErrorCalculated = absDiff / (std::min(absDoubleLeft + absDoubleRight, std::numeric_limits<double>::max()));
        const auto allowedError = relativeErrorCalculated < EPSILON;
        if (not allowedError)
        {
            NES_TRACE(
                "Relative error {} is greater than allowed error {} for values {} and {}",
                relativeErrorCalculated,
                EPSILON,
                doubleLeft,
                doubleRight);
        }
        return allowedError;
    }
    else if constexpr (std::is_same_v<T, std::string> || std::is_integral_v<T>)
    {
        return left == right;
    }
    else
    {
        throw InvalidDynamicCast("Unknown type {}", NAMEOF_TYPE(T));
    }
}

template <typename T, typename Tag>
class ResultCheckStrongType
{
public:
    explicit constexpr ResultCheckStrongType(T value) : value(std::move(value)) { }

    friend std::ostream& operator<<(std::ostream& os, const ResultCheckStrongType& strongType) { return os << strongType.getRawValue(); }

    [[nodiscard]] const T& getRawValue() const { return value; }

    [[nodiscard]] T& getRawValue() { return value; }

private:
    T value;
};

using ExpectedResultField = ResultCheckStrongType<std::string, struct ExpectedResultFields_>;
using ActualResultField = ResultCheckStrongType<std::string, struct ActualResultFields_>;

template <typename FieldType, typename Tag>
class ResultTuple
{
public:
    explicit ResultTuple(std::string tuple) : tuple(std::move(tuple)) { }

    [[nodiscard]] size_t size() const { return tuple.size(); }

    friend std::ostream& operator<<(std::ostream& os, const ResultTuple& resultTuple) { return os << resultTuple.tuple; }

    [[nodiscard]] const std::string& getRawValue() const { return tuple; }

    [[nodiscard]] std::vector<FieldType> getFields() const
    {
        return tuple | std::views::split(' ')
            | std::views::transform([](auto&& range) { return FieldType(std::string(range.begin(), range.end())); })
            | std::ranges::to<std::vector>();
    }

private:
    std::string tuple;
};

void sortOnFields(std::vector<std::string>& results, const std::vector<size_t>& fieldIdxs)
{
    std::ranges::sort(
        results,
        [&fieldIdxs](const std::string& lhs, const std::string& rhs)
        {
            for (const size_t fieldIdx : fieldIdxs)
            {
                const auto lhsField = std::string_view((lhs | std::views::split(' ') | std::views::drop(fieldIdx)).front());
                const auto rhsField = std::string_view((rhs | std::views::split(' ') | std::views::drop(fieldIdx)).front());

                if (lhsField == rhsField)
                {
                    continue;
                }
                return lhsField < rhsField;
            }
            /// All fields are equal
            return false;
        });
}

template <typename TupleIdxType, typename Tag>
class ResultTuples
{
public:
    explicit ResultTuples(std::vector<std::string> results, const std::vector<size_t>& expectedResultsFieldSortIdxs)
        : results(std::move(results))
    {
        /// An empty column becomes NULL, so it has a value to compare.
        for (auto& line : this->results)
        {
            auto tokens = line | std::views::split(',')
                | std::views::transform(
                              [](auto&& rng)
                              {
                                  const std::string token(rng.begin(), rng.end());
                                  return token.empty() ? std::string{"NULL"} : token;
                              })
                | std::ranges::to<std::vector>();
            line = fmt::format("{}", fmt::join(tokens, ","));
        }

        /// Both sides may separate fields with commas, so both drop them.
        /// Double spaces go too, because a single space separates the fields.
        std::ranges::for_each(this->results, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
        std::ranges::for_each(this->results, removeDoubleSpaces);

        sortOnFields(this->results, expectedResultsFieldSortIdxs);
    }

    using TupleType = Tag;

    [[nodiscard]] TupleType getTuple(const TupleIdxType tupleIdx) const { return TupleType{results.at(tupleIdx.getRawValue())}; }

    [[nodiscard]] size_t size() const { return results.size(); }

private:
    std::vector<std::string> results;
};

using ExpectedResultIndex = NESStrongType<uint64_t, struct ExpectedResultIndex_, 0, 1>;
using ActualResultIndex = NESStrongType<uint64_t, struct ActualResultIndex_, 0, 1>;
using ExpectedResultTuple = ResultTuple<ExpectedResultField, struct ExpectedResultTuple_>;
using ActualResultTuple = ResultTuple<ActualResultField, struct ActualResultTuple_>;
using ExpectedResultTuples = ResultTuples<ExpectedResultIndex, ExpectedResultTuple>;
using ActualResultTuples = ResultTuples<ActualResultIndex, ActualResultTuple>;
using ExpectedResultSchema = ResultCheckStrongType<Schema<UnqualifiedUnboundField, Ordered>, struct ExpectedResultSchema_>;
using ActualResultSchema = ResultCheckStrongType<Schema<UnqualifiedUnboundField, Ordered>, struct ActualResultSchema_>;
}
}

FMT_OSTREAM(NES::ExpectedResultTuple);
FMT_OSTREAM(NES::ActualResultTuple);
FMT_OSTREAM(NES::ActualResultField);
FMT_OSTREAM(NES::ExpectedResultField);

namespace NES
{
namespace
{
/// Reads a boolean written either as an integer, where anything but 0 is true, or as `true` or `false`.
bool convertToBool(const std::string& str)
{
    const auto lower = toLowerCase(str);
    if (lower == "true")
    {
        return true;
    }
    if (lower == "false")
    {
        return false;
    }
    const auto boolInt = from_chars<int>(str);
    INVARIANT(boolInt.has_value(), "Cannot convert '{}' to bool", str);
    return static_cast<bool>(boolInt.value());
}

bool compareStringAsTypeWithError(const DataType::Type type, const ExpectedResultField& left, const ActualResultField& right)
{
    const auto leftLower = toLowerCase(left.getRawValue());
    const auto rightLower = toLowerCase(right.getRawValue());
    /// Two NULL values are equal.
    if (leftLower == "null" and rightLower == "null")
    {
        return true;
    }
    if (leftLower == "null" or rightLower == "null")
    {
        return false;
    }

    switch (type)
    {
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::CHAR:
        case DataType::Type::VARSIZED:
            return left.getRawValue() == right.getRawValue();
        case DataType::Type::BOOLEAN: {
            const auto leftBool = convertToBool(left.getRawValue());
            const auto rightBool = convertToBool(right.getRawValue());
            return leftBool == rightBool;
        }
        case DataType::Type::FLOAT32:
            return compareStringAsTypeWithError<float>(left.getRawValue(), right.getRawValue());
        case DataType::Type::FLOAT64:
            return compareStringAsTypeWithError<double>(left.getRawValue(), right.getRawValue());
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting UNDEFINED in result check comparison");
    }
    std::unreachable();
}

struct ExpectedToActualFieldMap
{
    struct TypeIndexPair
    {
        DataType type;
        std::optional<size_t> actualIndex;
    };

    std::string schemaError;
    std::vector<size_t> expectedResultsFieldSortIdx;
    std::vector<size_t> actualResultsFieldSortIdx;
    std::vector<TypeIndexPair> expectedToActualFieldMap;
    std::vector<size_t> additionalActualFields;
};

class LineIndexIterator
{
public:
    LineIndexIterator(const size_t expectedResultLinesSize, const size_t actualResultLinesSize)
        : expectedResultLinesSize(expectedResultLinesSize)
        , actualResultLinesSize(actualResultLinesSize)
        , totalResultLinesSize(expectedResultLinesSize + actualResultLinesSize)
    {
    }

    [[nodiscard]] bool hasNext() const
    {
        return (expectedResultTupleIdx.getRawValue() + actualResultTupleIdx.getRawValue()) < totalResultLinesSize;
    }

    [[nodiscard]] ExpectedResultIndex getExpected() const { return expectedResultTupleIdx; }

    [[nodiscard]] ActualResultIndex getActual() const { return actualResultTupleIdx; }

    void advanceExpected() { this->expectedResultTupleIdx = ExpectedResultIndex{this->expectedResultTupleIdx.getRawValue() + 1}; }

    void advanceActual() { this->actualResultTupleIdx = ActualResultIndex{this->actualResultTupleIdx.getRawValue() + 1}; }

    [[nodiscard]] bool hasOnlyExpectedLinesLeft() const
    {
        return expectedResultTupleIdx < expectedResultLinesSize and actualResultTupleIdx >= actualResultLinesSize;
    }

    [[nodiscard]] bool hasOnlyActualLinesLeft() const
    {
        return actualResultTupleIdx < actualResultLinesSize and expectedResultTupleIdx >= expectedResultLinesSize;
    }

private:
    ExpectedResultIndex expectedResultTupleIdx = ExpectedResultIndex{0};
    ActualResultIndex actualResultTupleIdx = ActualResultIndex{0};
    ExpectedResultIndex expectedResultLinesSize = ExpectedResultIndex{0};
    ActualResultIndex actualResultLinesSize = ActualResultIndex{0};
    size_t totalResultLinesSize = 0;
};

ExpectedToActualFieldMap compareSchemas(const ExpectedResultSchema& expectedResultSchema, const ActualResultSchema& actualResultSchema)
{
    /// The expected side is the schema that the test declared for the sink, and the actual side is the header that the sink wrote.
    /// A name that the test wrote unquoted has no case of its own, so it matches the written name whatever case that has.
    const auto fieldsMatch = [](const auto& expectedField, const auto& actualField)
    {
        if (expectedField.getDataType() != actualField.getDataType())
        {
            return false;
        }
        const auto& expectedIdentifier = *expectedField.getFullyQualifiedName().begin();
        const auto& actualIdentifier = *actualField.getFullyQualifiedName().begin();
        if (expectedIdentifier.isCaseSensitive())
        {
            return expectedIdentifier == actualIdentifier;
        }
        return expectedIdentifier.asCanonicalString() == toUpperCase(actualIdentifier.asCanonicalString());
    };

    ExpectedToActualFieldMap expectedToActualFieldMap{};
    /// Unequal schemas open the schema error with both of them.
    if (not std::ranges::equal(expectedResultSchema.getRawValue(), actualResultSchema.getRawValue(), fieldsMatch))
    {
        expectedToActualFieldMap.schemaError += fmt::format(
            "\n{} != {}", fmt::join(expectedResultSchema.getRawValue(), ", "), fmt::join(actualResultSchema.getRawValue(), ", "));
    }
    /// Fields pair up by position, because the column order is part of the query's semantics.
    /// A column at the wrong position reports as missing on one side and unexpected on the other.
    const auto& expectedFields = expectedResultSchema.getRawValue();
    const auto& actualFields = actualResultSchema.getRawValue();
    for (const auto& [expectedFieldIdx, expectedField] : expectedFields | views::enumerate)
    {
        if (const auto idx = static_cast<size_t>(expectedFieldIdx);
            idx < std::ranges::size(actualFields) and fieldsMatch(expectedField, *actualFields[idx]))
        {
            expectedToActualFieldMap.expectedToActualFieldMap.emplace_back(expectedField.getDataType(), idx);
            expectedToActualFieldMap.expectedResultsFieldSortIdx.emplace_back(idx);
            expectedToActualFieldMap.actualResultsFieldSortIdx.emplace_back(idx);
        }
        else
        {
            expectedToActualFieldMap.schemaError += fmt::format("\n- '{}' is missing from actual result schema.", expectedField);
            expectedToActualFieldMap.expectedToActualFieldMap.emplace_back(expectedField.getDataType(), std::nullopt);
        }
    }
    for (const auto& [actualFieldIdx, actualField] : actualFields | views::enumerate)
    {
        if (const auto idx = static_cast<size_t>(actualFieldIdx);
            idx >= std::ranges::size(expectedFields) or not fieldsMatch(*expectedFields[idx], actualField))
        {
            expectedToActualFieldMap.schemaError += fmt::format("\n+ '{}' is unexpected field in actual result schema.", actualField);
            expectedToActualFieldMap.additionalActualFields.emplace_back(idx);
        }
    }
    return expectedToActualFieldMap;
}

enum class FieldMatchResult : uint8_t
{
    ALL_FIELDS_MATCHED,
    ALL_EXISTING_FIELD_MATCHED,
    AT_LEAST_ONE_FIELD_MISMATCHED,
};

/// Compares the expected fields of one row against the actual ones.
/// Reports that all matched, that the ones with a counterpart matched, or that at least one differed.
FieldMatchResult compareMatchableExpectedFields(
    const ExpectedToActualFieldMap& expectedToActualFieldMap,
    const std::vector<ExpectedResultField>& splitExpectedResult,
    const std::vector<ActualResultField>& splitActualResult)
{
    auto fieldMatchResult = FieldMatchResult::ALL_FIELDS_MATCHED;
    for (const auto& [expectedIdx, typeActualPair] : expectedToActualFieldMap.expectedToActualFieldMap | views::enumerate)
    {
        const auto& expectedField = splitExpectedResult.at(expectedIdx);
        if (typeActualPair.actualIndex.has_value())
        {
            const auto& actualField = splitActualResult.at(typeActualPair.actualIndex.value());
            if (not compareStringAsTypeWithError(typeActualPair.type.type, expectedField, actualField))
            {
                return FieldMatchResult::AT_LEAST_ONE_FIELD_MISMATCHED;
            }
        }
        else
        {
            fieldMatchResult = FieldMatchResult::ALL_EXISTING_FIELD_MATCHED;
        }
    }
    return fieldMatchResult;
}

void populateErrorWithMatchingFields(
    std::string& resultError,
    const ExpectedToActualFieldMap& expectedToActualFieldMap,
    const std::vector<ExpectedResultField>& splitExpectedResult,
    const std::vector<ActualResultField>& splitActualResult,
    LineIndexIterator& lineIdxIt)
{
    std::vector<std::string> expectedFields;
    std::vector<std::string> actualFields;
    for (const auto& [expectedIdx, typeActualPair] : expectedToActualFieldMap.expectedToActualFieldMap | views::enumerate)
    {
        expectedFields.push_back(fmt::to_string(splitExpectedResult.at(expectedIdx)));
        actualFields.push_back(
            typeActualPair.actualIndex.has_value() ? fmt::to_string(splitActualResult.at(typeActualPair.actualIndex.value())) : "_");
    }
    for (const auto& additionalIdx : expectedToActualFieldMap.additionalActualFields)
    {
        expectedFields.emplace_back("_");
        actualFields.push_back(fmt::to_string(splitActualResult.at(additionalIdx)));
    }
    resultError += fmt::format("\n{} | {}", fmt::join(expectedFields, " "), fmt::join(actualFields, " "));
    lineIdxIt.advanceExpected();
    lineIdxIt.advanceActual();
}

bool compareTuples(
    std::string& resultError,
    const ExpectedResultTuple& expectedResultLine,
    const ActualResultTuple& actualResultLine,
    const ExpectedToActualFieldMap& expectedToActualFieldMap,
    LineIndexIterator& lineIdxIt)
{
    if (expectedResultLine.getRawValue() == actualResultLine.getRawValue())
    {
        resultError += fmt::format("\n{} | {}", expectedResultLine, actualResultLine);
        lineIdxIt.advanceExpected();
        lineIdxIt.advanceActual();
        return true;
    }

    /// The lines differ as text, but their values may still be equal.
    const auto splitExpected = expectedResultLine.getFields();
    const auto splitActualResult = actualResultLine.getFields();

    if (splitExpected.size() != expectedToActualFieldMap.expectedToActualFieldMap.size())
    {
        lineIdxIt.advanceExpected();
        resultError += fmt::format(
            "\n{} | {}",
            expectedResultLine,
            fmt::format(
                "{} (expected sink schema has: {}, but got {})",
                ((splitExpected.size() < expectedToActualFieldMap.expectedToActualFieldMap.size()) ? "Not enough expected fields"
                                                                                                   : "Too many expected fields"),
                expectedToActualFieldMap.expectedToActualFieldMap.size(),
                splitExpected.size()));
        return false;
    }

    const bool hasSameNumberOfFields = (splitExpected.size() == splitActualResult.size());
    switch (compareMatchableExpectedFields(expectedToActualFieldMap, splitExpected, splitActualResult))
    {
        case FieldMatchResult::ALL_FIELDS_MATCHED: {
            if (hasSameNumberOfFields)
            {
                resultError += fmt::format("\n{} | {}", expectedResultLine, actualResultLine);
                lineIdxIt.advanceExpected();
                lineIdxIt.advanceActual();
                return true;
            }
            populateErrorWithMatchingFields(resultError, expectedToActualFieldMap, splitExpected, splitActualResult, lineIdxIt);
            return false;
        }
        case FieldMatchResult::ALL_EXISTING_FIELD_MATCHED: {
            populateErrorWithMatchingFields(resultError, expectedToActualFieldMap, splitExpected, splitActualResult, lineIdxIt);
            return false;
        }
        case FieldMatchResult::AT_LEAST_ONE_FIELD_MISMATCHED: {
            if (expectedResultLine.getRawValue() < actualResultLine.getRawValue())
            {
                resultError += fmt::format("\n{} | {}", expectedResultLine, std::string(expectedResultLine.size(), '_'));
                lineIdxIt.advanceExpected();
            }
            else
            {
                resultError += fmt::format("\n{} | {}", std::string(actualResultLine.size(), '_'), actualResultLine);
                lineIdxIt.advanceActual();
            }
            return false;
        }
    }
    std::unreachable();
}

std::string compareResults(
    const ExpectedResultTuples& formattedExpectedResultLines,
    const ActualResultTuples& formattedActualResultLines,
    const ExpectedToActualFieldMap& expectedToActualFieldMap)
{
    std::string resultError;

    bool allResultTuplesMatch = true;
    LineIndexIterator lineIdxIt{formattedExpectedResultLines.size(), formattedActualResultLines.size()};
    while (lineIdxIt.hasNext())
    {
        if (lineIdxIt.hasOnlyExpectedLinesLeft())
        {
            const auto& expectedLine = formattedExpectedResultLines.getTuple(lineIdxIt.getExpected());
            resultError += fmt::format("\n{} | {}", expectedLine, std::string(expectedLine.size(), '_'));
            lineIdxIt.advanceExpected();
            allResultTuplesMatch = false;
            continue;
        }
        if (lineIdxIt.hasOnlyActualLinesLeft())
        {
            const auto& actualLine = formattedActualResultLines.getTuple(lineIdxIt.getActual());
            resultError += fmt::format("\n{} | {}", std::string(actualLine.size(), '_'), actualLine);
            lineIdxIt.advanceActual();
            allResultTuplesMatch = false;
            continue;
        }
        /// Both sides still have lines, so compare the next pair.
        allResultTuplesMatch &= compareTuples(
            resultError,
            formattedExpectedResultLines.getTuple(lineIdxIt.getExpected()),
            formattedActualResultLines.getTuple(lineIdxIt.getActual()),
            expectedToActualFieldMap,
            lineIdxIt);
    }
    /// A comparison that matched still collected every line for the side-by-side listing, and none of it is an error.
    return allResultTuplesMatch ? std::string{} : resultError;
}

struct QuerySchemasAndResults
{
    explicit QuerySchemasAndResults(
        ExpectedResultSchema expectedSchema,
        ActualResultSchema actualSchema,
        std::vector<std::string> expectedQueryResult,
        std::vector<std::string> actualQueryResult)
        : expectedSchema(std::move(expectedSchema))
        , actualSchema(std::move(actualSchema))
        , expectedToActualResultMap(compareSchemas(this->expectedSchema, this->actualSchema))
        , expectedResults(ExpectedResultTuples(std::move(expectedQueryResult), this->expectedToActualResultMap.expectedResultsFieldSortIdx))
        , actualResults(ActualResultTuples(std::move(actualQueryResult), this->expectedToActualResultMap.actualResultsFieldSortIdx))
    {
    }

    const ExpectedResultTuples& getExpectedResultTuples() const { return expectedResults; }

    const ActualResultTuples& getActualResultTuples() const { return actualResults; }

    [[nodiscard]] const ExpectedToActualFieldMap& getExpectedToActualResultMap() const { return expectedToActualResultMap; }

    [[nodiscard]] const std::string& getSchemaError() const { return expectedToActualResultMap.schemaError; }

private:
    ExpectedResultSchema expectedSchema;
    ActualResultSchema actualSchema;
    ExpectedToActualFieldMap expectedToActualResultMap;
    ExpectedResultTuples expectedResults;
    ActualResultTuples actualResults;
};


}

ComparisonOutcome compare(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows)
{
    const QuerySchemasAndResults querySchemasAndResults{
        ExpectedResultSchema{expectedSchema}, ActualResultSchema{actualSchema}, expectedRows, actualRows};
    const auto resultError = compareResults(
        querySchemasAndResults.getExpectedResultTuples(),
        querySchemasAndResults.getActualResultTuples(),
        querySchemasAndResults.getExpectedToActualResultMap());
    return ComparisonOutcome{.schemaError = querySchemasAndResults.getSchemaError(), .resultError = resultError};
}

Verdict toVerdict(const ComparisonOutcome& outcome, const ComparisonOrigin origin)
{
    static constexpr std::string_view SchemaMismatchHeader = "Schema Mismatch\n"
                                                             "---------------";
    static constexpr std::string_view ResultMismatchHeader = "Result Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n"
                                                             "-------------------------------------------------";
    const bool schemaMismatch = not outcome.schemaError.empty();
    const bool resultMismatch = not outcome.resultError.empty();
    if (not schemaMismatch and not resultMismatch)
    {
        return Success{};
    }

    std::vector<std::string> blocks;
    if (schemaMismatch)
    {
        blocks.emplace_back(fmt::format("{}{}", SchemaMismatchHeader, outcome.schemaError));
    }
    if (schemaMismatch and not resultMismatch)
    {
        blocks.emplace_back("All Results match");
    }
    if (resultMismatch)
    {
        blocks.emplace_back(fmt::format("{}{}", ResultMismatchHeader, outcome.resultError));
    }
    if (origin == ComparisonOrigin::DifferentialBlock)
    {
        blocks.emplace_back("This error happened during differential query execution.");
    }
    return std::unexpected(Mismatch{fmt::to_string(fmt::join(blocks, "\n\n"))});
}

}
