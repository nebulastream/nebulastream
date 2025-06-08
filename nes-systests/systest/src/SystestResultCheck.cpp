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
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheckStrongTypes.hpp>
#include <SystestState.hpp>


namespace
{

NES::Systest::SystestSchema parseFieldNames(const std::string_view fieldNamesRawLine)
{
    /// Assumes the field and type to be similar to
    /// window$val_i8_i8:INT32, window$val_i8_i8_plus_1:INT16
    NES::Systest::SystestSchema fields;
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
        fields.emplace_back(dataType, std::string(nameTrimmed));
    }
    return fields;
}

std::optional<NES::Systest::QueryResult> loadQueryResult(const NES::Systest::SystestQuery& query)
{
    NES_DEBUG("Loading query result for query: {} from queryResultFile: {}", query.queryDefinition, query.resultFile());
    std::ifstream resultFile(query.resultFile());
    if (!resultFile)
    {
        NES_FATAL_ERROR("Failed to open result file: {}", query.resultFile());
        return std::nullopt;
    }

    NES::Systest::QueryResult result;
    std::string firstLine;
    if (!std::getline(resultFile, firstLine))
    {
        return result;
    }

    if (firstLine.find('$') != std::string::npos)
    {
        result.fields = parseFieldNames(firstLine);
    }

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

    SchemaErrorStream schemaErrorStream{};
    size_t expectedResultsFieldSortIdx = 0;
    size_t actualResultsFieldSortIdx = 0;
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
    bool hasSetSortFieldIdx = false;
    for (const auto& [expectedFieldIdx, expectedField] : expectedResultSchema.getRawValue() | NES::views::enumerate)
    {
        if (const auto& matchingFieldIt = std::ranges::find(actualResultSchema.getRawValue(), expectedField);
            matchingFieldIt != actualResultSchema.getRawValue().end())
        {
            auto offset = std::ranges::distance(actualResultSchema.getRawValue().begin(), matchingFieldIt);
            expectedToActualFieldMap.expectedToActualFieldMap.emplace_back(expectedField.type, offset);
            matchedActualResultFields.emplace(offset);
            if (not hasSetSortFieldIdx)
            {
                expectedToActualFieldMap.expectedResultsFieldSortIdx = expectedFieldIdx;
                expectedToActualFieldMap.actualResultsFieldSortIdx = offset;
                hasSetSortFieldIdx = true;
            }
        }
        else
        {
            expectedToActualFieldMap.schemaErrorStream << fmt::format("\n- '{}' is missing from actual result schema.", expectedField);
            expectedToActualFieldMap.expectedToActualFieldMap.emplace_back(expectedField.type, std::nullopt);
        }
    }
    for (size_t i = 0; i < actualResultSchema.getRawValue().size(); ++i)
    {
        if (not matchedActualResultFields.contains(i))
        {
            expectedToActualFieldMap.schemaErrorStream
                << fmt::format("\n+ '{}' is unexpected field in actual result schema.", actualResultSchema.getRawValue().at(i));
            expectedToActualFieldMap.additionalActualFields.emplace_back(i);
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
            if (typeActualPair.type.isType(NES::DataType::Type::FLOAT32)
                and not NES::Systest::compareStringAsTypeWithError<float>(expectedField.getRawValue(), actualField.getRawValue()))
            {
                return FieldMatchResult::AT_LEAST_ONE_FIELD_MISMATCHED;
            }
            if (typeActualPair.type.isType(NES::DataType::Type::FLOAT64)
                and not NES::Systest::compareStringAsTypeWithError<double>(expectedField.getRawValue(), actualField.getRawValue()))
            {
                return FieldMatchResult::AT_LEAST_ONE_FIELD_MISMATCHED;
            }
            if (expectedField.getRawValue() != actualField.getRawValue())
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

bool compareLines(
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
            "\n{} | Too few expected fields (sink schema has: {}, but got {})",
            expectedResultLine,
            expectedToActualFieldMap.expectedToActualFieldMap.size(),
            splitExpected.size());
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
    ResultErrorStream resultErrorStream;

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
        allResultTuplesMatch &= compareLines(
            resultErrorStream,
            formattedExpectedResultLines.getTuple(lineIdxIt.getExpected()),
            formattedActualResultLines.getTuple(lineIdxIt.getActual()),
            expectedToActualFieldMap,
            lineIdxIt);
    }
    if (allResultTuplesMatch)
    {
        return ResultErrorStream{};
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
    explicit QueryCheckResult(std::string queryErrorStream) : type(Type::QUERY_NOT_FOUND), queryError(std::move(queryErrorStream)) { }
    explicit QueryCheckResult(SchemaErrorStream schemaErrorStream, ResultErrorStream resultErrorStream)
        : schemaErrorStream(std::move(schemaErrorStream)), resultErrorStream(std::move(resultErrorStream))
    {
        if (this->schemaErrorStream.hasMismatch() and this->resultErrorStream.hasMismatch())
        {
            this->type = Type::SCHEMAS_MISMATCH_RESULTS_MISMATCH;
        }
        else if (this->schemaErrorStream.hasMismatch() and not(this->resultErrorStream.hasMismatch()))
        {
            this->type = Type::SCHEMAS_MISMATCH_RESULTS_MATCH;
        }
        else if (not(this->schemaErrorStream.hasMismatch()) and this->resultErrorStream.hasMismatch())
        {
            this->type = Type::SCHEMAS_MATCH_RESULTS_MISMATCH;
        }
        else if (not(this->schemaErrorStream.hasMismatch()) and not(this->resultErrorStream.hasMismatch()))
        {
            this->type = Type::SCHEMAS_MATCH_RESULTS_MATCH;
        }
    }

    Type type;
    std::string queryError;
    SchemaErrorStream schemaErrorStream;
    ResultErrorStream resultErrorStream;
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
        , expectedResults(
              std::move(ExpectedResultTuples(std::move(expectedQueryResult), this->expectedToActualResultMap.expectedResultsFieldSortIdx)))
        , actualResults(
              std::move(ActualResultTuples(std::move(actualQueryResult), this->expectedToActualResultMap.actualResultsFieldSortIdx)))
    {
    }

    const ExpectedResultTuples& getExpectedResultTuples() const { return expectedResults; }
    const ActualResultTuples& getActualResultTuples() const { return actualResults; }

    [[nodiscard]] const ExpectedToActualFieldMap& getExpectedToActualResultMap() const { return expectedToActualResultMap; }
    [[nodiscard]] SchemaErrorStream&& takeSchemaErrorStream() { return std::move(expectedToActualResultMap.schemaErrorStream); }

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

    QuerySchemasAndResults querySchemasAndResults = [&]()
    {
        auto [schemaResult, actualQueryResult] = queryResult.value();

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
            ExpectedResultSchema(std::move(runningQuery.query.expectedSinkSchema)),
            ActualResultSchema(std::move(schemaResult)),
            std::move(expectedQueryResult),
            std::move(actualQueryResult));
    }();

    /// Compare the expected and actual result schema and the expected and actual result lines/tuples
    auto resultComparisonErrorStream = compareResults(
        querySchemasAndResults.getExpectedResultTuples(),
        querySchemasAndResults.getActualResultTuples(),
        querySchemasAndResults.getExpectedToActualResultMap());

    return QueryCheckResult{std::move(querySchemasAndResults.takeSchemaErrorStream()), std::move(resultComparisonErrorStream)};
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
            return fmt::format("{}\n\nAll Results match", SchemaMismatchMessage, checkQueryResult.schemaErrorStream);
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
