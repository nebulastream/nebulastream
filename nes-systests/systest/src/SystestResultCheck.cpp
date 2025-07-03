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
#include <fstream>
#include <iterator>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
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
        /// We allow commas in the result and the expected result. To ensure they are equal we remove them from both.
        /// Additionally, we remove double spaces, as we expect a single space between the fields
        std::ranges::for_each(this->results, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
        std::ranges::for_each(this->results, NES::Util::removeDoubleSpaces);

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
using ExpectedResultSchema = ResultCheckStrongType<NES::Schema, struct ExpectedResultSchema_>;
using ActualResultSchema = ResultCheckStrongType<NES::Schema, struct ActualResultSchema_>;
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
bool compareStringAsTypeWithError(const NES::DataType::Type type, const ExpectedResultField& left, const ActualResultField& right)
{
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
        case NES::DataType::Type::BOOLEAN:
        case NES::DataType::Type::CHAR:
        case NES::DataType::Type::VARSIZED:
        case NES::DataType::Type::VARSIZED_POINTER_REP:
            return left.getRawValue() == right.getRawValue();
        case NES::DataType::Type::FLOAT32:
            return NES::Systest::compareStringAsTypeWithError<float>(left.getRawValue(), right.getRawValue());
        case NES::DataType::Type::FLOAT64:
            return NES::Systest::compareStringAsTypeWithError<double>(left.getRawValue(), right.getRawValue());
        case NES::DataType::Type::UNDEFINED:
            throw NES::UnknownDataType("Not supporting UNDEFINED in result check comparison");
    }
    std::unreachable();
}

NES::Schema parseFieldNames(const std::string_view fieldNamesRawLine)
{
    /// Assumes the field and type to be similar to
    /// window$val_i8_i8:INT32, window$val_i8_i8_plus_1:INT16
    NES::Schema schema;
    for (const auto& field : std::ranges::split_view(fieldNamesRawLine, ',')
             | std::views::transform([](auto splitNameAndType)
                                     { return std::string_view(splitNameAndType.begin(), splitNameAndType.end()); })
             | std::views::filter([](const auto& stringViewSplit) { return !stringViewSplit.empty(); }))
    {
        /// At this point, we have a field and tpye separated by a colon, e.g., "window$val_i8_i8:INT32"
        /// We need to split the fieldName and type by the colon, store the field name and type in a vector.
        /// After that, we can trim the field name and type and store it in the fields vector.
        /// "window$val_i8_i8:INT32 " -> ["window$val_i8_i8", "INT32 "] -> {INT32, "window$val_i8_i8"}
        const auto [nameTrimmed, typeTrimmed] = [](const std::string_view field) -> std::pair<std::string_view, std::string_view>
        {
            std::vector<std::string_view> fieldAndTypeVector;
            for (const auto subrange : std::ranges::split_view(field, ':'))
            {
                fieldAndTypeVector.emplace_back(NES::Util::trimWhiteSpaces(std::string_view(subrange)));
            }
            INVARIANT(fieldAndTypeVector.size() == 2, "Field and type pairs should always be pairs of a key and a value");
            return std::make_pair(fieldAndTypeVector.at(0), fieldAndTypeVector.at(1));
        }(field);
        NES::DataType dataType;
        if (auto type = magic_enum::enum_cast<NES::DataType::Type>(typeTrimmed); type.has_value())
        {
            dataType = NES::DataTypeProvider::provideDataType(type.value());
        }
        else if (NES::Util::toLowerCase(typeTrimmed) == "varsized")
        {
            dataType = NES::DataTypeProvider::provideDataType(NES::DataType::Type::VARSIZED);
        }
        else
        {
            throw NES::SLTUnexpectedToken("Unknown basic type: {}", typeTrimmed);
        }
        schema.addField(std::string(nameTrimmed), dataType);
    }
    return schema;
}


struct QueryResult
{
    NES::Schema schema;
    std::vector<std::string> result;
};
std::optional<QueryResult> loadQueryResult(const NES::Systest::SystestQuery& query)
{
    NES_DEBUG("Loading query result for query: {} from queryResultFile: {}", query.queryDefinition, query.resultFile());
    std::ifstream resultFile(query.resultFile());
    if (!resultFile)
    {
        NES_FATAL_ERROR("Failed to open result file: {}", query.resultFile());
        return std::nullopt;
    }

    QueryResult result;
    std::string firstLine;
    if (!std::getline(resultFile, firstLine))
    {
        return result;
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
    /// Check if schemas are equal. If not populate the error stream
    if (/* hasMatchingSchema */ expectedResultSchema.getRawValue() != actualResultSchema.getRawValue())
    {
        expectedToActualFieldMap.schemaErrorStream << fmt::format(
            "\n{} != {}", fmt::join(expectedResultSchema.getRawValue(), ", "), fmt::join(actualResultSchema.getRawValue(), ", "));
    }
    std::unordered_set<size_t> matchedActualResultFields;
    for (const auto& [expectedFieldIdx, expectedField] : expectedResultSchema.getRawValue() | NES::views::enumerate)
    {
        if (const auto& matchingFieldIt = std::ranges::find(actualResultSchema.getRawValue(), expectedField);
            matchingFieldIt != actualResultSchema.getRawValue().end())
        {
            auto offset = std::ranges::distance(actualResultSchema.getRawValue().begin(), matchingFieldIt);
            expectedToActualFieldMap.expectedToActualFieldMap.emplace_back(expectedField.dataType, offset);
            matchedActualResultFields.emplace(offset);
            expectedToActualFieldMap.expectedResultsFieldSortIdx.emplace_back(expectedFieldIdx);
            expectedToActualFieldMap.actualResultsFieldSortIdx.emplace_back(offset);
        }
        else
        {
            expectedToActualFieldMap.schemaErrorStream << fmt::format("\n- '{}' is missing from actual result schema.", expectedField);
            expectedToActualFieldMap.expectedToActualFieldMap.emplace_back(expectedField.dataType, std::nullopt);
        }
    }
    for (size_t fieldIdx = 0; fieldIdx < actualResultSchema.getRawValue().getNumberOfFields(); ++fieldIdx)
    {
        if (not matchedActualResultFields.contains(fieldIdx))
        {
            expectedToActualFieldMap.schemaErrorStream << fmt::format(
                "\n+ '{}' is unexpected field in actual result schema.", actualResultSchema.getRawValue().getFieldAt(fieldIdx));
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

QueryCheckResult checkQuery(const NES::Systest::RunningQuery& runningQuery, const NES::Systest::QueryResultMap& queryResultMap)
{
    /// Get result for running query
    const auto queryResult = loadQueryResult(runningQuery.query);
    if (not queryResult.has_value())
    {
        return QueryCheckResult{fmt::format("Failed to load query result for query: ", runningQuery.query.queryDefinition)};
    }

    const QuerySchemasAndResults querySchemasAndResults = [&]()
    {
        auto [actualSchemaResult, actualQueryResult] = queryResult.value();

        /// Check if the expected result is empty and if this is the case, the query result should be empty as well
        auto expectedQueryResult = [](const NES::Systest::RunningQuery& query, const NES::Systest::QueryResultMap& resultMap)
        {
            const auto currentQuery = resultMap.find(query.query.resultFile());
            INVARIANT(
                currentQuery != resultMap.end(),
                "Could not find query {}:{} in result map.",
                query.query.sqlLogicTestFile,
                query.query.queryIdInFile);
            return currentQuery->second;
        }(runningQuery, queryResultMap);

        return QuerySchemasAndResults(
            ExpectedResultSchema(runningQuery.query.expectedSinkSchema),
            ActualResultSchema(actualSchemaResult),
            std::move(expectedQueryResult),
            std::move(actualQueryResult));
    }();

    /// Compare the expected and actual result schema and the expected and actual result lines/tuples
    const auto resultComparisonErrorStream = compareResults(
        querySchemasAndResults.getExpectedResultTuples(),
        querySchemasAndResults.getActualResultTuples(),
        querySchemasAndResults.getExpectedToActualResultMap());

    return QueryCheckResult{querySchemasAndResults.getSchemaErrorStream(), resultComparisonErrorStream};
}
}

namespace NES::Systest
{

std::optional<std::string> checkResult(const RunningQuery& runningQuery, const QueryResultMap& queryResultMap)
{
    PRECONDITION(
        runningQuery.query.queryIdInFile.getRawValue() <= queryResultMap.size(),
        "No results for query with id {}. Only {} results available.",
        runningQuery.query.queryIdInFile,
        queryResultMap.size());

    static constexpr std::string_view SchemaMismatchMessage = "\n\n"
                                                              "Schema Mismatch\n"
                                                              "---------------";
    static constexpr std::string_view ResultMismatchMessage = "\n\n"
                                                              "Result Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n"
                                                              "-------------------------------------------------";

    switch (const auto checkQueryResult = checkQuery(runningQuery, queryResultMap); checkQueryResult.type)
    {
        case QueryCheckResult::Type::QUERY_NOT_FOUND: {
            return checkQueryResult.queryError;
        }
        case QueryCheckResult::Type::SCHEMAS_MATCH_RESULTS_MATCH: {
            return std::nullopt;
        }
        case QueryCheckResult::Type::SCHEMAS_MATCH_RESULTS_MISMATCH: {
            return fmt::format("{}{}", ResultMismatchMessage, checkQueryResult.resultErrorStream);
        }
        case QueryCheckResult::Type::SCHEMAS_MISMATCH_RESULTS_MATCH: {
            return fmt::format("{}{}\n\nAll Results match", SchemaMismatchMessage, checkQueryResult.schemaErrorStream);
        }
        case QueryCheckResult::Type::SCHEMAS_MISMATCH_RESULTS_MISMATCH: {
            return fmt::format(
                "{}{}{}{}",
                SchemaMismatchMessage,
                checkQueryResult.schemaErrorStream,
                ResultMismatchMessage,
                checkQueryResult.resultErrorStream);
        }
    }
    std::unreachable();
}
}
