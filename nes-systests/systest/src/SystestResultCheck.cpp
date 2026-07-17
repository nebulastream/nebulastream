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

#include <SystestResultCheck.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <ostream>
#include <ranges>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>

namespace
{
template <typename T, typename Tag>
class ResultCheckStrongType
{
public:
    explicit constexpr ResultCheckStrongType(const T value) : value(std::move(value)) { }

    using Underlying = T;
    using TypeTag = Tag;

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

    using TupleType = Tag;

    [[nodiscard]] size_t size() const { return tuple.size(); }

    friend std::ostream& operator<<(std::ostream& os, const ResultTuple& resultTuple) { return os << resultTuple.tuple; }

    [[nodiscard]] const std::string& getRawValue() const { return tuple; }

    [[nodiscard]] std::vector<FieldType> getFields() const
    {
        auto result = tuple | std::views::split(' ')
            | std::views::transform([](auto&& range) { return FieldType(std::string(range.begin(), range.end())); })
            | std::ranges::to<std::vector>();
        return result;
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
        /// We need to add NULL into each column that has no values to be able to compare it later on
        for (auto& line : this->results)
        {
            auto tokens = line | std::views::split(',')
                | std::views::transform(
                              [](auto&& rng)
                              {
                                  const std::string token(rng.begin(), rng.end());
                                  return token.empty() ? "NULL" : token;
                              });
            std::ostringstream oss;
            std::ranges::copy(tokens, std::ostream_iterator<std::string>(oss, ","));
            std::string s = oss.str();
            /// Removing trailing comma
            if (not s.empty() and s.back() == ',')
            {
                s.pop_back();
            }
            line = s;
        }

        /// We allow commas in the result and the expected result. To ensure they are equal we remove them from both.
        /// Additionally, we remove double spaces, as we expect a single space between the fields
        std::ranges::for_each(this->results, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
        std::ranges::for_each(this->results, NES::removeDoubleSpaces);

        sortOnFields(this->results, expectedResultsFieldSortIdxs);
    }

    ~ResultTuples() = default;
    using TupleType = Tag;

    [[nodiscard]] TupleType getTuple(const TupleIdxType tupleIdx) const { return TupleType(results.at(tupleIdx.getRawValue())); }

    [[nodiscard]] size_t size() const { return results.size(); }

private:
    std::vector<std::string> results;
};

template <typename ErrorStringType, typename Tag>
class ErrorStream
{
public:
    explicit ErrorStream(std::stringstream errorStream) : errorStream(std::move(errorStream)) { }

    using ErrorStreamType = Tag;

    bool hasMismatch() const { return not errorStream.view().empty(); }

    ErrorStringType getErrorString() const { return ErrorStringType(errorStream.str()); }

    friend std::ostream& operator<<(std::ostream& os, const ErrorStream& ses) { return os << ses.errorStream.str(); }

    template <typename T>
    ErrorStream& operator<<(T&& value)
    {
        errorStream << std::forward<T>(value);
        return *this;
    }

private:
    std::stringstream errorStream;
};

using ExpectedResultIndex = NES::NESStrongType<uint64_t, struct ExpectedResultIndex_, 0, 1>;
using ActualResultIndex = NES::NESStrongType<uint64_t, struct ActualResultIndex_, 0, 1>;
using ExpectedResultTuple = ResultTuple<ExpectedResultField, struct ExpectedResultTuple_>;
using ActualResultTuple = ResultTuple<ActualResultField, struct ActualResultTuple_>;
using ExpectedResultTuples = ResultTuples<ExpectedResultIndex, ExpectedResultTuple>;
using ActualResultTuples = ResultTuples<ActualResultIndex, ActualResultTuple>;
using ExpectedResultSchema = ResultCheckStrongType<NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>, struct ExpectedResultSchema_>;
using ActualResultSchema = ResultCheckStrongType<NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>, struct ActualResultSchema_>;
using SchemaErrorString = ResultCheckStrongType<std::string, struct SchemaErrorString_>;
using ResultErrorString = ResultCheckStrongType<std::string, struct ResultErrorString_>;
using SchemaErrorStream = ErrorStream<SchemaErrorString, struct SchemaErrorStream_>;
using ResultErrorStream = ErrorStream<ResultErrorString, struct ResultErrorStream_>;
}

FMT_OSTREAM(::SchemaErrorStream);
FMT_OSTREAM(::ResultErrorStream);
FMT_OSTREAM(::ExpectedResultTuple);
FMT_OSTREAM(::ActualResultTuple);
FMT_OSTREAM(::ActualResultField);
FMT_OSTREAM(::ExpectedResultField);
FMT_OSTREAM(::SchemaErrorString);
FMT_OSTREAM(::ResultErrorString);

namespace
{
/// We support bool being an integer (anything else than 0 is true) or "true" / "false"
bool convertToBool(const std::string& str)
{
    const auto lower = NES::toLowerCase(str);
    if (lower == "true")
    {
        return true;
    }
    if (lower == "false")
    {
        return false;
    }
    const auto boolInt = NES::from_chars<int>(str);
    INVARIANT(boolInt.has_value(), "Cannot convert '{}' to bool", str);
    return static_cast<bool>(boolInt.value());
}

bool compareStringAsTypeWithError(const NES::DataType::Type type, const ExpectedResultField& left, const ActualResultField& right)
{
    const auto leftLower = NES::toLowerCase(left.getRawValue());
    const auto rightLower = NES::toLowerCase(right.getRawValue());
    /// If both string values are NULL, they are equal
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
        case NES::DataType::Type::INT8:
        case NES::DataType::Type::INT16:
        case NES::DataType::Type::INT32:
        case NES::DataType::Type::INT64:
        case NES::DataType::Type::UINT8:
        case NES::DataType::Type::UINT16:
        case NES::DataType::Type::UINT32:
        case NES::DataType::Type::UINT64:
        case NES::DataType::Type::CHAR:
        case NES::DataType::Type::VARSIZED:
            return left.getRawValue() == right.getRawValue();
        case NES::DataType::Type::BOOLEAN: {
            const auto leftBool = convertToBool(left.getRawValue());
            const auto rightBool = convertToBool(right.getRawValue());
            return leftBool == rightBool;
        }
        case NES::DataType::Type::FLOAT32:
            return NES::compareStringAsTypeWithError<float>(left.getRawValue(), right.getRawValue());
        case NES::DataType::Type::FLOAT64:
            return NES::compareStringAsTypeWithError<double>(left.getRawValue(), right.getRawValue());
        case NES::DataType::Type::UNDEFINED:
            throw NES::UnknownDataType("Not supporting UNDEFINED in result check comparison");
    }
    std::unreachable();
}

NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered> parseFieldNames(const std::string_view fieldNamesRawLine)
{
    /// Assumes the field and type to be similar to
    /// window$val_i8_i8:INT32:IS_NULLABLE, window$val_i8_i8_plus_1:INT16:NOT_NULLABLE
    auto fields
        = std::ranges::split_view(fieldNamesRawLine, ',')
        | std::views::transform([](auto splitNameAndType) { return std::string_view(splitNameAndType.begin(), splitNameAndType.end()); })
        | std::views::filter([](const auto& stringViewSplit) { return !stringViewSplit.empty(); })
        | std::views::transform(
              [](const auto& field)
              {
                  /// At this point, we have a field and tpye separated by a colon, e.g., "window$val_i8_i8:INT32"
                  /// We need to split the fieldName and type by the colon, store the field name and type in a vector.
                  /// After that, we can trim the field name and type and store it in the fields vector.
                  /// "window$val_i8_i8:INT32:IS_NULLABLE " -> ["window$val_i8_i8", "INT32 ", " IS_NULLABLE"] -> {"window$val_i8_i8", INT32, NOT_NULLABLE}
                  const auto [nameTrimmed, typeTrimmed, isNullable]
                      = [](const std::string_view field) -> std::tuple<std::string_view, std::string_view, NES::DataType::NULLABLE>
                  {
                      std::vector<std::string_view> fieldAndTypeVector;
                      for (const auto subrange : std::ranges::split_view(field, ':'))
                      {
                          fieldAndTypeVector.emplace_back(NES::trimWhiteSpaces(std::string_view(subrange)));
                      }
                      INVARIANT(
                          fieldAndTypeVector.size() == 3, "Field and type pairs should always be pairs of a key, a value and isNullable");

                      const auto isNullableString = fieldAndTypeVector.at(2);
                      const auto isNullable = magic_enum::enum_cast<NES::DataType::NULLABLE>(isNullableString);
                      if (not isNullable)
                      {
                          throw NES::SLTUnexpectedToken("Unknown nullable: {}", isNullableString);
                      }
                      return std::make_tuple(fieldAndTypeVector.at(0), fieldAndTypeVector.at(1), isNullable.value());
                  }(field);
                  NES::DataType dataType;
                  if (auto type = magic_enum::enum_cast<NES::DataType::Type>(typeTrimmed); type.has_value())
                  {
                      dataType = NES::DataTypeProvider::provideDataType(type.value(), isNullable);
                  }
                  else if (NES::toLowerCase(typeTrimmed) == "varsized")
                  {
                      dataType = NES::DataTypeProvider::provideDataType(NES::DataType::Type::VARSIZED, isNullable);
                  }
                  else
                  {
                      throw NES::SLTUnexpectedToken("Unknown basic type: {}", typeTrimmed);
                  }
                  return NES::UnqualifiedUnboundField{NES::Identifier::parse(std::string(nameTrimmed)), dataType};
              });
    return fields | std::ranges::to<NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>>();
}

struct QueryResult
{
    NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered> schema;
    std::vector<std::string> result;
};

std::optional<QueryResult> loadQueryResult(const std::filesystem::path& resultFilePath)
{
    NES_DEBUG("Loading query result from: {}", resultFilePath);
    std::ifstream resultFile(resultFilePath);
    if (!resultFile)
    {
        NES_ERROR("Failed to open result file: {}", resultFilePath);
        return std::nullopt;
    }

    QueryResult result;
    std::string firstLine;
    if (!std::getline(resultFile, firstLine))
    {
        NES_ERROR("Result file is empty", resultFilePath);
        return std::nullopt;
    }

    result.schema = parseFieldNames(firstLine);

    while (std::getline(resultFile, firstLine))
    {
        result.result.push_back(firstLine);
    }
    return result;
}

[[maybe_unused]] std::optional<QueryResult> loadQueryResult(const NES::Systest::SystestQuery& query)
{
    NES_DEBUG("Loading query result for query: {} from queryResultFile: {}", query.queryDefinition, query.resultFile());
    return loadQueryResult(query.resultFile());
}

struct ExpectedToActualFieldMap
{
    struct TypeIndexPair
    {
        NES::DataType type;
        std::optional<size_t> actualIndex;
    };

    SchemaErrorStream schemaErrorStream = SchemaErrorStream{std::stringstream{}};
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

    ~LineIndexIterator() = default;

    [[nodiscard]] bool hasNext() const
    {
        return (expectedResultTupleIdx.getRawValue() + actualResultTupleIdx.getRawValue()) < totalResultLinesSize;
    }

    [[nodiscard]] ExpectedResultIndex getExpected() const { return expectedResultTupleIdx; }

    [[nodiscard]] ActualResultIndex getActual() const { return actualResultTupleIdx; }

    void advanceExpected() { this->expectedResultTupleIdx = ExpectedResultIndex(this->expectedResultTupleIdx.getRawValue() + 1); }

    void advanceActual() { this->actualResultTupleIdx = ActualResultIndex(this->actualResultTupleIdx.getRawValue() + 1); }

    [[nodiscard]] bool hasOnlyExpectedLinesLeft() const
    {
        return expectedResultTupleIdx < expectedResultLinesSize and actualResultTupleIdx >= actualResultLinesSize;
    }

    [[nodiscard]] bool hasOnlyActualLinesLeft() const
    {
        return actualResultTupleIdx < actualResultLinesSize and expectedResultTupleIdx >= expectedResultLinesSize;
    }

private:
    ExpectedResultIndex expectedResultTupleIdx = ExpectedResultIndex(0);
    ActualResultIndex actualResultTupleIdx = ActualResultIndex(0);
    ExpectedResultIndex expectedResultLinesSize = ExpectedResultIndex(0);
    ActualResultIndex actualResultLinesSize = ActualResultIndex(0);
    size_t totalResultLinesSize = 0;
};

ExpectedToActualFieldMap compareSchemas(const ExpectedResultSchema& expectedResultSchema, const ActualResultSchema& actualResultSchema)
{
    ExpectedToActualFieldMap expectedToActualFieldMap{};
    /// Check if schemas are equal. If not populate the error stream
    if (/* hasMatchingSchema */ expectedResultSchema.getRawValue() != actualResultSchema.getRawValue())
    {
        expectedToActualFieldMap.schemaErrorStream << fmt::format(
            "\n{} != {}", fmt::join(expectedResultSchema.getRawValue(), ", "), fmt::join(actualResultSchema.getRawValue(), ", "));
    }
    std::unordered_set<size_t> matchedActualResultFields;
    for (const auto& [expectedFieldIdx, expectedField] : expectedResultSchema.getRawValue() | NES::views::enumerate)
    {
        /// Find a matching actual field that has not already been matched. This handles duplicate field names
        /// by matching each expected field to a distinct actual field in order.
        const auto& actualSchema = actualResultSchema.getRawValue();
        const auto begin = actualSchema.begin();
        const auto end = actualSchema.end();
        auto matchingFieldIt = end;
        for (auto it = begin; it != end; ++it)
        {
            const auto idx = static_cast<size_t>(std::distance(begin, it));
            if (*it == expectedField and not matchedActualResultFields.contains(idx))
            {
                matchingFieldIt = it;
                break;
            }
        }
        if (matchingFieldIt != end)
        {
            auto offset = static_cast<size_t>(std::distance(begin, matchingFieldIt));
            expectedToActualFieldMap.expectedToActualFieldMap.emplace_back(expectedField.getDataType(), offset);
            matchedActualResultFields.emplace(offset);
            expectedToActualFieldMap.expectedResultsFieldSortIdx.emplace_back(expectedFieldIdx);
            expectedToActualFieldMap.actualResultsFieldSortIdx.emplace_back(offset);
        }
        else
        {
            expectedToActualFieldMap.schemaErrorStream << fmt::format("\n- '{}' is missing from actual result schema.", expectedField);
            expectedToActualFieldMap.expectedToActualFieldMap.emplace_back(expectedField.getDataType(), std::nullopt);
        }
    }
    for (size_t fieldIdx = 0; fieldIdx < std::ranges::size(actualResultSchema.getRawValue()); ++fieldIdx)
    {
        if (not matchedActualResultFields.contains(fieldIdx))
        {
            expectedToActualFieldMap.schemaErrorStream
                << fmt::format("\n+ '{}' is unexpected field in actual result schema.", actualResultSchema.getRawValue()[fieldIdx]);
            expectedToActualFieldMap.additionalActualFields.emplace_back(fieldIdx);
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

/// Compares expected and actual result fields.
/// Returns 'ALL_EXISTING_FIELD_MATCHED' there was a one-to-one mapping between result and expected fields and all fields matched.
/// Returns 'ALL_EXISTING_FIELD_MATCHED' if there
FieldMatchResult compareMatchableExpectedFields(
    const ExpectedToActualFieldMap& expectedToActualFieldMap,
    const std::vector<ExpectedResultField>& splitExpectedResult,
    const std::vector<ActualResultField>& splitActualResult)
{
    auto fieldMatchResult = FieldMatchResult::ALL_FIELDS_MATCHED;
    for (const auto& [expectedIdx, typeActualPair] : expectedToActualFieldMap.expectedToActualFieldMap | NES::views::enumerate)
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
    ResultErrorStream& resultErrorStream,
    const ExpectedToActualFieldMap& expectedToActualFieldMap,
    const std::vector<ExpectedResultField>& splitExpectedResult,
    const std::vector<ActualResultField>& splitActualResult,
    LineIndexIterator& lineIdxIt)
{
    std::stringstream currentExpectedResultLineErrorStream;
    std::stringstream currentActualResultLineErrorStream;
    for (const auto& [expectedIdx, typeActualPair] : expectedToActualFieldMap.expectedToActualFieldMap | NES::views::enumerate)
    {
        const auto& expectedField = splitExpectedResult.at(expectedIdx);
        currentExpectedResultLineErrorStream << fmt::format("{} ", expectedField);
        if (typeActualPair.actualIndex.has_value())
        {
            const auto& actualField = splitActualResult.at(typeActualPair.actualIndex.value());
            currentActualResultLineErrorStream << fmt::format("{} ", actualField);
        }
        else
        {
            currentActualResultLineErrorStream << "_ ";
        }
    }
    for (const auto& additionalIdx : expectedToActualFieldMap.additionalActualFields)
    {
        currentExpectedResultLineErrorStream << "_ ";
        currentActualResultLineErrorStream << fmt::format("{} ", splitActualResult.at(additionalIdx));
    }
    resultErrorStream << fmt::format("\n{} | {}", currentExpectedResultLineErrorStream.str(), currentActualResultLineErrorStream.str());
    lineIdxIt.advanceExpected();
    lineIdxIt.advanceActual();
}

bool compareTuples(
    ResultErrorStream& resultErrorStream,
    const ExpectedResultTuple& expectedResultLine,
    const ActualResultTuple& actualResultLine,
    const ExpectedToActualFieldMap& expectedToActualFieldMap,
    LineIndexIterator& lineIdxIt)
{
    if (expectedResultLine.getRawValue() == actualResultLine.getRawValue())
    {
        resultErrorStream << fmt::format("\n{} | {}", expectedResultLine, actualResultLine);
        lineIdxIt.advanceExpected();
        lineIdxIt.advanceActual();
        return true;
    }

    /// The lines don't string-match, but they might still be equal
    const auto splitExpected = expectedResultLine.getFields();
    const auto splitActualResult = actualResultLine.getFields();

    if (splitExpected.size() != expectedToActualFieldMap.expectedToActualFieldMap.size())
    {
        lineIdxIt.advanceExpected();
        resultErrorStream << fmt::format(
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
                resultErrorStream << fmt::format("\n{} | {}", expectedResultLine, actualResultLine);
                lineIdxIt.advanceExpected();
                lineIdxIt.advanceActual();
                return true;
            }
            populateErrorWithMatchingFields(resultErrorStream, expectedToActualFieldMap, splitExpected, splitActualResult, lineIdxIt);
            return false;
        }
        case FieldMatchResult::ALL_EXISTING_FIELD_MATCHED: {
            populateErrorWithMatchingFields(resultErrorStream, expectedToActualFieldMap, splitExpected, splitActualResult, lineIdxIt);
            return false;
        }
        case FieldMatchResult::AT_LEAST_ONE_FIELD_MISMATCHED: {
            if (expectedResultLine.getRawValue() < actualResultLine.getRawValue())
            {
                resultErrorStream << fmt::format("\n{} | {}", expectedResultLine, std::string(expectedResultLine.size(), '_'));
                lineIdxIt.advanceExpected();
            }
            else
            {
                resultErrorStream << fmt::format("\n{} | {}", std::string(actualResultLine.size(), '_'), actualResultLine);
                lineIdxIt.advanceActual();
            }
            return false;
        }
    }
    std::unreachable();
}

ResultErrorStream compareResults(
    const ExpectedResultTuples& formattedExpectedResultLines,
    const ActualResultTuples& formattedActualResultLines,
    const ExpectedToActualFieldMap& expectedToActualFieldMap)
{
    ResultErrorStream resultErrorStream{std::stringstream{}};

    bool allResultTuplesMatch = true;
    LineIndexIterator lineIdxIt{formattedExpectedResultLines.size(), formattedActualResultLines.size()};
    while (lineIdxIt.hasNext())
    {
        if (lineIdxIt.hasOnlyExpectedLinesLeft())
        {
            const auto& expectedLine = formattedExpectedResultLines.getTuple(lineIdxIt.getExpected());
            resultErrorStream << fmt::format("\n{} | {}", expectedLine, std::string(expectedLine.size(), '_'));
            lineIdxIt.advanceExpected();
            allResultTuplesMatch = false;
            continue;
        }
        if (lineIdxIt.hasOnlyActualLinesLeft())
        {
            const auto& actualLine = formattedActualResultLines.getTuple(lineIdxIt.getActual());
            resultErrorStream << fmt::format("\n{} | {}", std::string(actualLine.size(), '_'), actualLine);
            lineIdxIt.advanceActual();
            allResultTuplesMatch = false;
            continue;
        }
        /// Both sets still have lines check if the lines are equal
        allResultTuplesMatch &= compareTuples(
            resultErrorStream,
            formattedExpectedResultLines.getTuple(lineIdxIt.getExpected()),
            formattedActualResultLines.getTuple(lineIdxIt.getActual()),
            expectedToActualFieldMap,
            lineIdxIt);
    }
    if (allResultTuplesMatch)
    {
        return ResultErrorStream{std::stringstream{}};
    }
    return resultErrorStream;
}

struct QueryCheckResult
{
    enum class Type : uint8_t
    {
        SCHEMAS_MISMATCH_RESULTS_MISMATCH,
        SCHEMAS_MISMATCH_RESULTS_MATCH,
        SCHEMAS_MATCH_RESULTS_MISMATCH,
        SCHEMAS_MATCH_RESULTS_MATCH,
        QUERY_NOT_FOUND,
    };

    explicit QueryCheckResult(std::string queryErrorStream)
        : type(Type::QUERY_NOT_FOUND), queryError(std::move(queryErrorStream)), schemaErrorStream(""), resultErrorStream("")
    {
    }

    explicit QueryCheckResult(const SchemaErrorStream& schemaErrorStream, const ResultErrorStream& resultErrorStream)
        : schemaErrorStream(schemaErrorStream.getErrorString()), resultErrorStream(resultErrorStream.getErrorString())
    {
        if (schemaErrorStream.hasMismatch() and resultErrorStream.hasMismatch())
        {
            this->type = Type::SCHEMAS_MISMATCH_RESULTS_MISMATCH;
        }
        else if (schemaErrorStream.hasMismatch() and not(resultErrorStream.hasMismatch()))
        {
            this->type = Type::SCHEMAS_MISMATCH_RESULTS_MATCH;
        }
        else if (not(schemaErrorStream.hasMismatch()) and resultErrorStream.hasMismatch())
        {
            this->type = Type::SCHEMAS_MATCH_RESULTS_MISMATCH;
        }
        else if (not(schemaErrorStream.hasMismatch()) and not(resultErrorStream.hasMismatch()))
        {
            this->type = Type::SCHEMAS_MATCH_RESULTS_MATCH;
        }
    }

    Type type;
    std::string queryError;
    SchemaErrorString schemaErrorStream;
    ResultErrorString resultErrorStream;
};

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

    [[nodiscard]] const SchemaErrorStream& getSchemaErrorStream() const { return expectedToActualResultMap.schemaErrorStream; }

private:
    ExpectedResultSchema expectedSchema;
    ActualResultSchema actualSchema;
    ExpectedToActualFieldMap expectedToActualResultMap;
    ExpectedResultTuples expectedResults;
    ActualResultTuples actualResults;
};

QueryCheckResult checkQuery(const NES::Systest::RunningQuery& runningQuery)
{
    /// Get result for running query
    const auto queryResult = loadQueryResult(runningQuery.systestQuery);
    if (not queryResult.has_value())
    {
        return QueryCheckResult{fmt::format("Failed to load query result for query: {}", runningQuery.systestQuery.queryDefinition)};
    }

    const QuerySchemasAndResults querySchemasAndResults = [&]()
    {
        auto [actualSchemaResult, actualQueryResult] = queryResult.value();

        /// Check if the expected result is empty and if this is the case, the query result should be empty as well
        auto expectedQueryResult = runningQuery.systestQuery.expectedResultsOrExpectedError;
        INVARIANT(std::holds_alternative<std::vector<std::string>>(expectedQueryResult), "Systest was expected to have an expected result");

        return QuerySchemasAndResults(
            ExpectedResultSchema(runningQuery.systestQuery.planInfoOrException.value().sinkOutputSchema),
            ActualResultSchema(actualSchemaResult),
            std::get<std::vector<std::string>>(expectedQueryResult),
            std::move(actualQueryResult));
    }();

    /// Compare the expected and actual result schema and the expected and actual result lines/tuples
    const auto resultComparisonErrorStream = compareResults(
        querySchemasAndResults.getExpectedResultTuples(),
        querySchemasAndResults.getActualResultTuples(),
        querySchemasAndResults.getExpectedToActualResultMap());

    return QueryCheckResult{querySchemasAndResults.getSchemaErrorStream(), resultComparisonErrorStream};
}

static constexpr std::string_view RegexOpen = "<REGEX>";
static constexpr std::string_view RegexClose = "</REGEX>";
static constexpr std::string_view NegativeRegexOpen = "<!REGEX>";
static constexpr std::string_view NegativeRegexClose = "</!REGEX>";

struct ExplainRegexTags
{
    std::string_view opening;
    std::string_view closing;
    bool shouldMatch;
};

struct ExplainRegexAssertion
{
    std::string pattern{};
    bool shouldMatch = false;
    size_t line = 0;
};

bool containsExplainRegexTag(const std::string_view line)
{
    return line.contains(RegexOpen) || line.contains(RegexClose) || line.contains(NegativeRegexOpen) || line.contains(NegativeRegexClose);
}

std::optional<ExplainRegexTags> explainRegexTags(const std::string_view line)
{
    if (line.starts_with(RegexOpen))
    {
        return ExplainRegexTags{.opening = RegexOpen, .closing = RegexClose, .shouldMatch = true};
    }
    if (line.starts_with(NegativeRegexOpen))
    {
        return ExplainRegexTags{.opening = NegativeRegexOpen, .closing = NegativeRegexClose, .shouldMatch = false};
    }
    return std::nullopt;
}

std::string explainRegexSyntaxError(const size_t line, const std::string_view message)
{
    return fmt::format("\n\nInvalid Explain Regex Assertion (line {}): {}", line + 1, message);
}

std::expected<ExplainRegexAssertion, std::string>
parseSingleLineExplainRegex(const std::string_view line, const ExplainRegexTags tags, const size_t assertionLine)
{
    if (!line.ends_with(tags.closing))
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, fmt::format("inline assertion must end with {}", tags.closing)));
    }

    const auto pattern = line.substr(tags.opening.size(), line.size() - tags.opening.size() - tags.closing.size());
    if (containsExplainRegexTag(pattern))
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, "nested or mismatched regex tag"));
    }
    if (pattern.empty())
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, "regex must not be empty"));
    }
    return ExplainRegexAssertion{.pattern = std::string{pattern}, .shouldMatch = tags.shouldMatch, .line = assertionLine};
}

std::expected<ExplainRegexAssertion, std::string>
parseMultilineExplainRegex(const std::vector<std::string>& expected, size_t& expectedLineIndex, const ExplainRegexTags tags)
{
    const auto assertionLine = expectedLineIndex++;
    std::string pattern;
    while (expectedLineIndex < expected.size() && expected[expectedLineIndex] != tags.closing)
    {
        if (containsExplainRegexTag(expected[expectedLineIndex]))
        {
            return std::unexpected(explainRegexSyntaxError(expectedLineIndex, "nested or mismatched regex tag"));
        }
        if (!pattern.empty())
        {
            pattern += '\n';
        }
        pattern += expected[expectedLineIndex++];
    }

    if (expectedLineIndex == expected.size())
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, fmt::format("missing closing tag {}", tags.closing)));
    }
    ++expectedLineIndex;
    if (pattern.empty())
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, "regex must not be empty"));
    }
    return ExplainRegexAssertion{.pattern = std::move(pattern), .shouldMatch = tags.shouldMatch, .line = assertionLine};
}

std::expected<std::vector<ExplainRegexAssertion>, std::string> parseExplainRegexAssertions(const std::vector<std::string>& expected)
{
    std::vector<ExplainRegexAssertion> assertions;
    size_t expectedLineIndex = 0;
    while (expectedLineIndex < expected.size())
    {
        const auto line = std::string_view{expected[expectedLineIndex]};
        const auto tags = explainRegexTags(line);
        if (!tags)
        {
            return std::unexpected(explainRegexSyntaxError(expectedLineIndex, "tagged and untagged expected output must not be mixed"));
        }

        std::expected<ExplainRegexAssertion, std::string> assertion = line == tags->opening
            ? parseMultilineExplainRegex(expected, expectedLineIndex, *tags)
            : parseSingleLineExplainRegex(line, *tags, expectedLineIndex++);
        if (!assertion)
        {
            return std::unexpected(std::move(assertion).error());
        }
        assertions.emplace_back(std::move(assertion).value());
    }
    return assertions;
}

std::optional<std::string>
checkExplainRegexAssertions(const std::vector<ExplainRegexAssertion>& assertions, const std::string& actualOutput)
{
    for (const auto& assertion : assertions)
    {
        try
        {
            const auto matches = std::regex_search(actualOutput, std::regex(assertion.pattern));
            if (matches != assertion.shouldMatch)
            {
                return fmt::format(
                    "\n\n"
                    "Explain Output Regex Assertion Failed (line {}, expected pattern \"{}\" {} match)\n"
                    "----------------------\n"
                    "Actual:\n{}",
                    assertion.line + 1,
                    assertion.pattern,
                    assertion.shouldMatch ? "to" : "not to",
                    actualOutput);
            }
        }
        catch (const std::regex_error& exception)
        {
            return fmt::format(
                "\n\nInvalid Explain Output Regex (line {}, pattern \"{}\"): {}", assertion.line + 1, assertion.pattern, exception.what());
        }
    }
    return std::nullopt;
}
}

namespace NES
{
std::optional<std::string> checkResult(const Systest::RunningQuery& runningQuery)
{
    /// Void sinks discard all tuples and produce no result file, so there is nothing to check.
    if (runningQuery.systestQuery.planInfoOrException.has_value())
    {
        const auto sinkOperators
            = getOperatorByType<SinkLogicalOperator>(runningQuery.systestQuery.planInfoOrException.value().queryPlan.getGlobalPlan());
        if (not sinkOperators.empty())
        {
            if (const auto sinkOp = sinkOperators.at(0).tryGetAs<SinkLogicalOperator>(); sinkOp.has_value()
                and sinkOp.value()->getSinkDescriptor().has_value()
                and toUpperCase(sinkOp.value()->getSinkDescriptor().value().getSinkType()) == "VOID")
            {
                NES_INFO(
                    "Skipping result check for {}:{} because it writes to a Void sink.",
                    runningQuery.systestQuery.testName,
                    runningQuery.systestQuery.queryIdInFile);
                return std::nullopt;
            }
        }
    }

    static constexpr std::string_view SchemaMismatchMessage = "\n\n"
                                                              "Schema Mismatch\n"
                                                              "---------------";
    static constexpr std::string_view ResultMismatchMessage = "\n\n"
                                                              "Result Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n"
                                                              "-------------------------------------------------";

    const auto annotateDifferentialError = [&](std::string message) -> std::string
    {
        if (runningQuery.systestQuery.differentialQueryPlan.has_value())
        {
            if (not message.empty())
            {
                message.append("\n");
            }
            message.append("\nThis error happend during differential query execution.");
        }
        return message;
    };

    QueryCheckResult checkQueryResult{""};

    if (runningQuery.systestQuery.differentialQueryPlan.has_value())
    {
        const auto result1 = loadQueryResult(runningQuery.systestQuery.resultFile());
        const auto result2 = loadQueryResult(runningQuery.systestQuery.resultFileForDifferentialQuery());

        if (not result1)
        {
            return annotateDifferentialError(fmt::format(
                "Failed to load first result file for differential query comparison: {}", runningQuery.systestQuery.resultFile()));
        }

        if (not result2)
        {
            return annotateDifferentialError(fmt::format(
                "Failed to load second result file for differential query comparison: {}",
                runningQuery.systestQuery.resultFileForDifferentialQuery()));
        }

        if (std::ranges::size(result1->schema) == 0)
        {
            return annotateDifferentialError(
                fmt::format("First result file is empty or has no schema: {}", runningQuery.systestQuery.resultFile()));
        }

        if (std::ranges::size(result2->schema) == 0)
        {
            return annotateDifferentialError(fmt::format(
                "Second result file is empty or has no schema: {}", runningQuery.systestQuery.resultFileForDifferentialQuery()));
        }

        const QuerySchemasAndResults querySchemasAndResults = [&]()
        {
            auto [schema1, result1Data] = result1.value();
            auto [schema2, result2Data] = result2.value();

            return QuerySchemasAndResults(
                ExpectedResultSchema(schema1), ActualResultSchema(schema2), std::move(result1Data), std::move(result2Data));
        }();

        /// Compare the schemas and results using the normal result check logic
        const auto resultComparisonErrorStream = compareResults(
            querySchemasAndResults.getExpectedResultTuples(),
            querySchemasAndResults.getActualResultTuples(),
            querySchemasAndResults.getExpectedToActualResultMap());

        checkQueryResult = QueryCheckResult{querySchemasAndResults.getSchemaErrorStream(), resultComparisonErrorStream};
    }
    else
    {
        checkQueryResult = checkQuery(runningQuery);
    }

    switch (checkQueryResult.type)
    {
        case QueryCheckResult::Type::QUERY_NOT_FOUND: {
            return annotateDifferentialError(checkQueryResult.queryError);
        }
        case QueryCheckResult::Type::SCHEMAS_MATCH_RESULTS_MATCH: {
            return std::nullopt;
        }
        case QueryCheckResult::Type::SCHEMAS_MATCH_RESULTS_MISMATCH: {
            return annotateDifferentialError(fmt::format("{}{}", ResultMismatchMessage, checkQueryResult.resultErrorStream));
        }
        case QueryCheckResult::Type::SCHEMAS_MISMATCH_RESULTS_MATCH: {
            return annotateDifferentialError(
                fmt::format("{}{}\n\nAll Results match", SchemaMismatchMessage, checkQueryResult.schemaErrorStream));
        }
        case QueryCheckResult::Type::SCHEMAS_MISMATCH_RESULTS_MISMATCH: {
            return annotateDifferentialError(fmt::format(
                "{}{}{}{}",
                SchemaMismatchMessage,
                checkQueryResult.schemaErrorStream,
                ResultMismatchMessage,
                checkQueryResult.resultErrorStream));
        }
    }
    std::unreachable();
}

std::optional<std::string> checkExplainResult(const Systest::RunningQuery& runningQuery)
{
    INVARIANT(runningQuery.systestQuery.actualExplainOutput.has_value(), "checkExplainResult requires a computed explain output");

    const auto rightTrim = [](const std::string_view line) -> std::string
    {
        const auto end = line.find_last_not_of(" \t\r");
        return std::string{line.substr(0, end == std::string_view::npos ? 0 : end + 1)};
    };

    /// Right-trim every line and drop empty lines on both sides: indentation is significant, but expected blocks in
    /// test files cannot contain empty lines (an empty line terminates the block).
    const auto normalize = [&rightTrim](auto&& inputLines) -> std::vector<std::string>
    {
        std::vector<std::string> normalized;
        for (auto&& line : inputLines)
        {
            if (auto trimmed = rightTrim(line); not trimmed.empty())
            {
                normalized.push_back(std::move(trimmed));
            }
        }
        return normalized;
    };

    const auto* expectedResultLines = std::get_if<std::vector<std::string>>(&runningQuery.systestQuery.expectedResultsOrExpectedError);
    INVARIANT(expectedResultLines != nullptr, "EXPLAIN statements must have expected result lines");

    const auto expected = normalize(*expectedResultLines);
    const auto actual = normalize(
        runningQuery.systestQuery.actualExplainOutput.value() | std::views::split('\n')
        | std::views::transform([](auto&& split) { return std::string_view(split.begin(), split.end()); }));

    if (std::ranges::any_of(expected, [](const auto& line) { return containsExplainRegexTag(line); }))
    {
        auto assertions = parseExplainRegexAssertions(expected);
        if (!assertions)
        {
            return std::move(assertions).error();
        }
        return checkExplainRegexAssertions(assertions.value(), fmt::format("{}", fmt::join(actual, "\n")));
    }

    if (expected == actual)
    {
        return std::nullopt;
    }

    auto firstDifferingLine = std::ranges::mismatch(expected, actual).in1 - expected.begin();

    static constexpr std::string_view EndOfOutput = "<end of output>";
    return fmt::format(
        "\n\n"
        "Explain Output Mismatch (first difference at line {}, expected \"{}\" but got \"{}\")\n"
        "----------------------\n"
        "Expected:\n{}\n\n"
        "Actual:\n{}",
        firstDifferingLine + 1,
        std::cmp_less(firstDifferingLine, expected.size()) ? std::string_view{expected[firstDifferingLine]} : EndOfOutput,
        std::cmp_less(firstDifferingLine, actual.size()) ? std::string_view{actual[firstDifferingLine]} : EndOfOutput,
        fmt::join(expected, "\n"),
        fmt::join(actual, "\n"));
}
}
