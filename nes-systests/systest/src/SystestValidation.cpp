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

#include <SystestValidation.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
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
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

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
    const auto fieldAt = [](const std::string& row, const size_t fieldIdx) -> std::optional<std::string_view>
    {
        auto fields = row | std::views::split(' ');
        auto field = fields.begin();
        for (size_t index = 0; index < fieldIdx && field != fields.end(); ++index)
        {
            ++field;
        }
        if (field == fields.end())
        {
            return std::nullopt;
        }
        const auto range = *field;
        return std::string_view{range.begin(), range.end()};
    };
    std::ranges::sort(
        results,
        [&fieldIdxs, &fieldAt](const std::string& lhs, const std::string& rhs)
        {
            for (const size_t fieldIdx : fieldIdxs)
            {
                const auto lhsField = fieldAt(lhs, fieldIdx);
                const auto rhsField = fieldAt(rhs, fieldIdx);

                if (lhsField == rhsField)
                {
                    continue;
                }
                return lhsField < rhsField;
            }
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
            if (not s.empty() and s.back() == ',')
            {
                s.pop_back();
            }
            line = s;
        }

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
template <typename T>
bool compareFloatingPointStrings(const std::string& left, const std::string& right)
{
    static constexpr auto EPSILON = 1e-5;
    size_t parsedLeft = 0;
    size_t parsedRight = 0;
    const auto doubleLeft = std::stod(left, &parsedLeft);
    const auto doubleRight = std::stod(right, &parsedRight);
    if (parsedLeft != left.size() || parsedRight != right.size())
    {
        return false;
    }
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
            return compareFloatingPointStrings<float>(left.getRawValue(), right.getRawValue());
        case NES::DataType::Type::FLOAT64:
            return compareFloatingPointStrings<double>(left.getRawValue(), right.getRawValue());
        case NES::DataType::Type::UNDEFINED:
            throw NES::UnknownDataType("Not supporting UNDEFINED in result check comparison");
    }
    std::unreachable();
}

NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered> parseFieldNames(const std::string_view fieldNamesRawLine)
{
    auto fields
        = std::ranges::split_view(fieldNamesRawLine, ',')
        | std::views::transform([](auto splitNameAndType) { return std::string_view(splitNameAndType.begin(), splitNameAndType.end()); })
        | std::views::filter([](const auto& stringViewSplit) { return !stringViewSplit.empty(); })
        | std::views::transform(
              [](const auto& field)
              {
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
    if (expectedResultSchema.getRawValue() != actualResultSchema.getRawValue())
    {
        expectedToActualFieldMap.schemaErrorStream << fmt::format(
            "\n{} != {}", fmt::join(expectedResultSchema.getRawValue(), ", "), fmt::join(actualResultSchema.getRawValue(), ", "));
    }
    std::unordered_set<size_t> matchedActualResultFields;
    for (const auto& [expectedFieldIdx, expectedField] : expectedResultSchema.getRawValue() | NES::views::enumerate)
    {
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
    const auto splitExpected = expectedResultLine.getFields();
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

    if (expectedResultLine.getRawValue() == actualResultLine.getRawValue())
    {
        resultErrorStream << fmt::format("\n{} | {}", expectedResultLine, actualResultLine);
        lineIdxIt.advanceExpected();
        lineIdxIt.advanceActual();
        return true;
    }

    const auto splitActualResult = actualResultLine.getFields();
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

constexpr std::string_view RegexOpen = "<REGEX>";
constexpr std::string_view RegexClose = "</REGEX>";
constexpr std::string_view NegativeRegexOpen = "<!REGEX>";
constexpr std::string_view NegativeRegexClose = "</!REGEX>";

struct ExplainRegexTags
{
    std::string_view opening;
    std::string_view closing;
    bool shouldMatch;
};

struct ExplainRegexAssertion
{
    std::string pattern;
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

namespace
{
NES::Systest::ComparisonResult comparisonResult(const QueryCheckResult& result, const bool differential)
{
    static constexpr std::string_view SchemaMismatchMessage = "\n\n"
                                                              "Schema Mismatch\n"
                                                              "---------------";
    static constexpr std::string_view ResultMismatchMessage = "\n\n"
                                                              "Result Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n"
                                                              "-------------------------------------------------";

    std::string message;
    switch (result.type)
    {
        case QueryCheckResult::Type::QUERY_NOT_FOUND:
            message = result.queryError;
            break;
        case QueryCheckResult::Type::SCHEMAS_MATCH_RESULTS_MATCH:
            return NES::Systest::ComparisonResult{.matches = true, .diagnostics = {}};
        case QueryCheckResult::Type::SCHEMAS_MATCH_RESULTS_MISMATCH:
            message = fmt::format("{}{}", ResultMismatchMessage, result.resultErrorStream);
            break;
        case QueryCheckResult::Type::SCHEMAS_MISMATCH_RESULTS_MATCH:
            message = fmt::format("{}{}\n\nAll Results match", SchemaMismatchMessage, result.schemaErrorStream);
            break;
        case QueryCheckResult::Type::SCHEMAS_MISMATCH_RESULTS_MISMATCH:
            message
                = fmt::format("{}{}{}{}", SchemaMismatchMessage, result.schemaErrorStream, ResultMismatchMessage, result.resultErrorStream);
            break;
    }
    if (differential)
    {
        if (!message.empty())
        {
            message.push_back('\n');
        }
        message.append("\nThis error happend during differential query execution.");
    }
    return NES::Systest::ComparisonResult{
        .matches = false,
        .diagnostics = {{.kind = NES::Systest::DiagnosticKind::Validation, .message = std::move(message), .source = std::nullopt}}};
}

NES::Systest::ComparisonResult compareDecodedTables(
    const NES::Systest::DecodedInputStream& expected, const NES::Systest::DecodedInputStream& actual, const bool differential)
{
    const QuerySchemasAndResults querySchemasAndResults(
        ExpectedResultSchema(expected.schema), ActualResultSchema(actual.schema), expected.rows, actual.rows);
    const auto resultComparisonErrorStream = compareResults(
        querySchemasAndResults.getExpectedResultTuples(),
        querySchemasAndResults.getActualResultTuples(),
        querySchemasAndResults.getExpectedToActualResultMap());
    return comparisonResult(QueryCheckResult{querySchemasAndResults.getSchemaErrorStream(), resultComparisonErrorStream}, differential);
}

std::vector<std::string> normalizeTextLines(auto&& inputLines)
{
    std::vector<std::string> normalized;
    for (auto&& line : inputLines)
    {
        const auto lineView = std::string_view{line};
        const auto end = lineView.find_last_not_of(" \t\r");
        auto trimmed = std::string{lineView.substr(0, end == std::string_view::npos ? 0 : end + 1)};
        if (!trimmed.empty())
        {
            normalized.push_back(std::move(trimmed));
        }
    }
    return normalized;
}
}

namespace NES::Systest
{
std::expected<DecodedTable, ValidationDiagnostic> FileResultDecoder::decode(const TableArtifact& artifact) const
{
    auto result = loadQueryResult(artifact.file);
    if (!result)
    {
        return std::unexpected(ValidationDiagnostic{
            .kind = DiagnosticKind::Validation,
            .message = fmt::format("Failed to load query result from {}", artifact.file),
            .source = std::nullopt});
    }
    return DecodedTable{.schema = std::move(result->schema), .rows = std::move(result->result)};
}

ComparisonResult
ResultComparator::compare(const RowsExpectation& expected, const ResultSchema& schema, const DecodedInputStream& actual) const
{
    switch (expected.comparison)
    {
        case ComparisonPolicy::UnorderedTypedRows:
            return compareDecodedTables(DecodedInputStream{.schema = schema, .rows = expected.rows}, actual, false);
    }
    return ComparisonResult{
        .matches = false,
        .diagnostics
        = {{.kind = DiagnosticKind::Validation,
            .message = fmt::format("Unsupported row comparison policy {}", static_cast<uint8_t>(expected.comparison)),
            .source = std::nullopt}}};
}

ComparisonResult ResultComparator::compare(const DecodedInputStream& expected, const DecodedInputStream& actual) const
{
    return compareDecodedTables(expected, actual, true);
}

ComparisonResult TextComparator::compare(const TextExpectation& expectedText, const std::string_view actualText) const
{
    const auto expected = normalizeTextLines(expectedText.lines);
    const auto actual = normalizeTextLines(
        actualText | std::views::split('\n')
        | std::views::transform([](auto&& split) { return std::string_view(split.begin(), split.end()); }));
    const auto actualOutput = fmt::format("{}", fmt::join(actual, "\n"));
    const auto useRegex = expectedText.matching == TextMatchPolicy::RegexAssertions
        || (expectedText.matching == TextMatchPolicy::Automatic
            && std::ranges::any_of(expected, [](const auto& line) { return containsExplainRegexTag(line); }));
    if (useRegex)
    {
        auto assertions = parseExplainRegexAssertions(expected);
        if (!assertions)
        {
            return ComparisonResult{
                .matches = false,
                .diagnostics = {{.kind = DiagnosticKind::Validation, .message = std::move(assertions).error(), .source = std::nullopt}}};
        }
        if (auto error = checkExplainRegexAssertions(*assertions, actualOutput))
        {
            return ComparisonResult{
                .matches = false,
                .diagnostics = {{.kind = DiagnosticKind::Validation, .message = std::move(*error), .source = std::nullopt}}};
        }
        return ComparisonResult{.matches = true, .diagnostics = {}};
    }
    if (expected == actual)
    {
        return ComparisonResult{.matches = true, .diagnostics = {}};
    }

    const auto firstDifferingLine = static_cast<size_t>(std::ranges::mismatch(expected, actual).in1 - expected.begin());
    static constexpr std::string_view EndOfOutput = "<end of output>";
    return ComparisonResult{
        .matches = false,
        .diagnostics
        = {{.kind = DiagnosticKind::Validation,
            .message = fmt::format(
                "\n\n"
                "Explain Output Mismatch (first difference at line {}, expected \"{}\" but got \"{}\")\n"
                "----------------------\n"
                "Expected:\n{}\n\n"
                "Actual:\n{}",
                firstDifferingLine + 1,
                std::cmp_less(firstDifferingLine, expected.size()) ? std::string_view{expected[firstDifferingLine]} : EndOfOutput,
                std::cmp_less(firstDifferingLine, actual.size()) ? std::string_view{actual[firstDifferingLine]} : EndOfOutput,
                fmt::join(expected, "\n"),
                actualOutput),
            .source = std::nullopt}}};
}

namespace
{
ExecutionMetrics metricsFrom(const ExecutionOutcome& outcome)
{
    if (const auto* completed = std::get_if<CompletedExecution>(&outcome))
    {
        return completed->metrics;
    }
    return {};
}

ArtifactSet artifactsFrom(const ExecutionOutcome& outcome)
{
    if (const auto* completed = std::get_if<CompletedExecution>(&outcome))
    {
        return completed->artifacts;
    }
    if (const auto* failed = std::get_if<FailedExecution>(&outcome))
    {
        return failed->artifacts;
    }
    if (const auto* timedOut = std::get_if<TimedOutExecution>(&outcome))
    {
        return timedOut->artifacts;
    }
    return {};
}
}

CaseValidator::CaseValidator(
    const ResultDecoder& decoder,
    const ResultComparator& resultComparator,
    const TextComparator& textComparator,
    const PreparedExecutionCatalog& preparedExecutions)
    : decoder(decoder), resultComparator(resultComparator), textComparator(textComparator), preparedExecutions(preparedExecutions)
{
}

ValidatedResult CaseValidator::validate(const ResolvedCase& testCase, const ExecutionOutcome& outcome) const
try
{
    ValidatedResult result{
        .id = testCase.id,
        .verdict = Verdict::Failed,
        .diagnostics = {},
        .metrics = metricsFrom(outcome),
        .artifacts = artifactsFrom(outcome)};
    const auto fail = [&](std::string message, const DiagnosticKind kind = DiagnosticKind::Validation)
    {
        result.verdict = Verdict::Failed;
        result.diagnostics.push_back(Diagnostic{.kind = kind, .message = std::move(message), .source = testCase.source});
    };
    const auto applyComparison = [&](ComparisonResult comparison)
    {
        result.verdict = comparison.matches ? Verdict::Passed : Verdict::Failed;
        for (auto& diagnostic : comparison.diagnostics)
        {
            diagnostic.source = testCase.source;
            result.diagnostics.push_back(std::move(diagnostic));
        }
    };

    if (const auto* skipped = std::get_if<SkippedExecution>(&outcome))
    {
        result.verdict = Verdict::Skipped;
        result.artifacts = {};
        if (skipped->reason)
        {
            result.diagnostics.push_back(
                Diagnostic{.kind = DiagnosticKind::Scheduling, .message = *skipped->reason, .source = testCase.source});
        }
        else if (!skipped->failedDependencies.empty())
        {
            result.diagnostics.push_back(Diagnostic{
                .kind = DiagnosticKind::Scheduling, .message = "Skipped because a dependency did not pass", .source = testCase.source});
        }
        return result;
    }
    if (const auto* timedOut = std::get_if<TimedOutExecution>(&outcome))
    {
        result.artifacts = timedOut->artifacts;
        fail(fmt::format("Case timed out after {} ms", timedOut->elapsed.count()), DiagnosticKind::Execution);
        return result;
    }
    if (const auto* failed = std::get_if<FailedExecution>(&outcome))
    {
        result.artifacts = failed->artifacts;
        if (const auto* expectedError = std::get_if<ErrorExpectation>(&testCase.expectation);
            expectedError && failed->error.kind == ExecutionErrorKind::Statement)
        {
            const auto matchingDetail = std::ranges::find_if(
                failed->error.details,
                [&](const ExecutionErrorDetail& detail)
                {
                    return detail.code == expectedError->code
                        && (!expectedError->message || detail.message.contains(*expectedError->message));
                });
            if (matchingDetail != failed->error.details.end())
            {
                result.verdict = Verdict::Passed;
                return result;
            }
            const auto expected = expectedError->message
                ? fmt::format("{} containing message \"{}\"", expectedError->code, *expectedError->message)
                : fmt::format("{}", expectedError->code);
            const auto actual = fmt::format(
                "{}",
                fmt::join(
                    failed->error.details
                        | std::views::transform(
                            [](const ExecutionErrorDetail& detail) { return fmt::format("{}: {}", detail.code, detail.message); }),
                    "; "));
            fail(fmt::format("Expected statement error {}. Actual details: {}", expected, actual));
            return result;
        }
        fail(fmt::format("Query Failed with unexpected error: {}", failed->error.message()), DiagnosticKind::Execution);
        return result;
    }

    const auto& completed = std::get<CompletedExecution>(outcome);
    result.metrics = completed.metrics;
    result.artifacts = completed.artifacts;
    if (const auto* expectedError = std::get_if<ErrorExpectation>(&testCase.expectation))
    {
        fail(fmt::format("expected error {} but query succeeded", expectedError->code));
        return result;
    }
    if (const auto* rows = std::get_if<RowsExpectation>(&testCase.expectation))
    {
        const auto& execution = preparedExecutions.at(testCase.id);
        const auto& prepared = *std::get<std::shared_ptr<const PreparedAction>>(execution.prepared);
        const auto& statement = std::get<PreparedQuery>(prepared).statement;
        if (statement.output.kind == OutputTargetKind::Discard)
        {
            result.verdict = Verdict::Passed;
            return result;
        }
        const auto output = std::ranges::find_if(
            completed.outputs,
            [](const StatementOutput& statementOutput) { return std::holds_alternative<TableArtifact>(statementOutput); });
        if (output == completed.outputs.end())
        {
            fail("Completed query did not produce a table artifact");
            return result;
        }
        auto decoded = decoder.decode(std::get<TableArtifact>(*output));
        if (!decoded)
        {
            fail(decoded.error().message);
            return result;
        }
        applyComparison(resultComparator.compare(*rows, statement.outputSchema, *decoded));
        return result;
    }
    if (std::holds_alternative<DifferentialExpectation>(testCase.expectation))
    {
        std::vector<TableArtifact> tables;
        for (const auto& output : completed.outputs)
        {
            if (const auto* table = std::get_if<TableArtifact>(&output))
            {
                tables.push_back(*table);
            }
        }
        if (tables.size() != 2)
        {
            fail("Differential execution did not produce two table artifacts");
            return result;
        }
        auto left = decoder.decode(tables[0]);
        if (!left)
        {
            fail(fmt::format(
                "Failed to load first result file for differential query comparison: {}\n\nThis error happend during differential "
                "query "
                "execution.",
                tables[0].file));
            return result;
        }
        auto right = decoder.decode(tables[1]);
        if (!right)
        {
            fail(fmt::format(
                "Failed to load second result file for differential query comparison: {}\n\nThis error happend during differential "
                "query "
                "execution.",
                tables[1].file));
            return result;
        }
        if (left->schema.size() == 0)
        {
            fail(fmt::format(
                "First result file is empty or has no schema: {}\n\nThis error happend during differential query execution.",
                tables[0].file));
            return result;
        }
        if (right->schema.size() == 0)
        {
            fail(fmt::format(
                "Second result file is empty or has no schema: {}\n\nThis error happend during differential query execution.",
                tables[1].file));
            return result;
        }
        applyComparison(resultComparator.compare(*left, *right));
        return result;
    }

    const auto& text = std::get<TextExpectation>(testCase.expectation);
    const auto output = std::ranges::find_if(
        completed.outputs, [](const StatementOutput& statementOutput) { return std::holds_alternative<TextArtifact>(statementOutput); });
    if (output == completed.outputs.end())
    {
        fail("Completed EXPLAIN statement did not produce text output");
        return result;
    }
    applyComparison(textComparator.compare(text, std::get<TextArtifact>(*output).text));
    return result;
}
catch (const Exception& exception)
{
    return ValidatedResult{
        .id = testCase.id,
        .verdict = Verdict::Failed,
        .diagnostics = {{.kind = DiagnosticKind::Validation, .message = exception.what(), .source = testCase.source}},
        .metrics = metricsFrom(outcome),
        .artifacts = artifactsFrom(outcome)};
}
catch (const std::exception& exception)
{
    return ValidatedResult{
        .id = testCase.id,
        .verdict = Verdict::Failed,
        .diagnostics = {{.kind = DiagnosticKind::Validation, .message = exception.what(), .source = testCase.source}},
        .metrics = metricsFrom(outcome),
        .artifacts = artifactsFrom(outcome)};
}

}
