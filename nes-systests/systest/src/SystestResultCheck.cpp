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
#include <DataTypes/Schema.hpp>
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

    std::stringstream schemaErrorStream;
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

    [[nodiscard]] bool hasNext() const { return (expectedResultTupleIdx + actualResultTupleIdx) < totalResultLinesSize; }

    [[nodiscard]] size_t getExpected() const { return expectedResultTupleIdx; }
    [[nodiscard]] size_t getActual() const { return actualResultTupleIdx; }
    void advanceExpected() { expectedResultTupleIdx++; }
    void advanceActual() { actualResultTupleIdx++; }

    [[nodiscard]] bool hasOnlyExpectedLinesLeft() const
    {
        return expectedResultTupleIdx < expectedResultLinesSize and actualResultTupleIdx >= actualResultLinesSize;
    }
    [[nodiscard]] bool hasOnlyActualLinesLeft() const
    {
        return actualResultTupleIdx < actualResultLinesSize and expectedResultTupleIdx >= expectedResultLinesSize;
    }

private:
    size_t expectedResultTupleIdx = 0;
    size_t actualResultTupleIdx = 0;
    size_t expectedResultLinesSize = 0;
    size_t actualResultLinesSize = 0;
    size_t totalResultLinesSize = 0;
};

ExpectedToActualFieldMap compareSchemas(const NES::Schema& expectedResultSchema, const NES::Schema& actualResultSchema)
{
    ExpectedToActualFieldMap expectedToActualFieldMap{};
    /// Check if schemas are equal. If not populate the error stream
    if (/* hasMatchingSchema */ expectedResultSchema != actualResultSchema)
    {
        expectedToActualFieldMap.schemaErrorStream
            << fmt::format("\n{} != {}", fmt::join(expectedResultSchema, ", "), fmt::join(actualResultSchema, ", "));
    }
    std::unordered_set<size_t> matchedActualResultFields;
    for (const auto& [expectedFieldIdx, expectedField] : expectedResultSchema | NES::views::enumerate)
    {
        if (const auto& matchingFieldIt = std::ranges::find(actualResultSchema, expectedField); matchingFieldIt != actualResultSchema.end())
        {
            auto offset = std::ranges::distance(actualResultSchema.begin(), matchingFieldIt);
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
    for (size_t fieldIdx = 0; fieldIdx < actualResultSchema.getNumberOfFields(); ++fieldIdx)
    {
        if (not matchedActualResultFields.contains(fieldIdx))
        {
            expectedToActualFieldMap.schemaErrorStream
                << fmt::format("\n+ '{}' is unexpected field in actual result schema.", actualResultSchema.getFieldAt(fieldIdx));
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
    const std::vector<std::string>& splitExpectedResult,
    const std::vector<std::string>& splitActualResult)
{
    auto fieldMatchResult = FieldMatchResult::ALL_FIELDS_MATCHED;
    for (const auto& [expectedIdx, typeActualPair] : expectedToActualFieldMap.expectedToActualFieldMap | NES::views::enumerate)
    {
        const auto& expectedField = splitExpectedResult.at(expectedIdx);
        if (typeActualPair.actualIndex.has_value())
        {
            const auto& actualField = splitActualResult.at(typeActualPair.actualIndex.value());
            if (not NES::Systest::compareStringAsTypeWithError(typeActualPair.type.type, expectedField, actualField))
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
    std::stringstream& errorStream,
    const ExpectedToActualFieldMap& expectedToActualFieldMap,
    const std::vector<std::string>& splitExpectedResult,
    const std::vector<std::string>& splitActualResult,
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
    errorStream << fmt::format("\n{} | {}", currentExpectedResultLineErrorStream.str(), currentActualResultLineErrorStream.str());
    lineIdxIt.advanceExpected();
    lineIdxIt.advanceActual();
}

bool compareLines(
    std::stringstream& lineComparisonErrorStream,
    const std::string& expectedResultLine,
    const std::string& actualResultLine,
    const ExpectedToActualFieldMap& expectedToActualFieldMap,
    LineIndexIterator& lineIdxIt)
{
    if (expectedResultLine == actualResultLine)
    {
        lineComparisonErrorStream << fmt::format("\n{} | {}", expectedResultLine, actualResultLine);
        lineIdxIt.advanceExpected();
        lineIdxIt.advanceActual();
        return true;
    }

    /// The lines don't string-match, but they might still be equal
    const auto splitExpected = NES::Util::splitWithStringDelimiter<std::string>(expectedResultLine, " ");
    const auto splitActualResult = NES::Util::splitWithStringDelimiter<std::string>(actualResultLine, " ");

    if (splitExpected.size() != expectedToActualFieldMap.expectedToActualFieldMap.size())
    {
        lineIdxIt.advanceExpected();
        lineComparisonErrorStream << fmt::format(
           "\n{} | {}",
           expectedResultLine,
           fmt::format(
               "{} (expected sink schema has: {}, but got {})",
               ((splitExpected.size() < expectedToActualFieldMap.expectedToActualFieldMap.size()) ? "Not enough expected fields"
                                                                                                  : "Too many expected fields"),
               expectedToActualFieldMap.expectedToActualFieldMap.size(),
               splitExpected.size())
           );
        return false;
    }

    const bool hasSameNumberOfFields = (splitExpected.size() == splitActualResult.size());
    switch (compareMatchableExpectedFields(expectedToActualFieldMap, splitExpected, splitActualResult))
    {
        case FieldMatchResult::ALL_FIELDS_MATCHED: {
            if (hasSameNumberOfFields)
            {
                lineComparisonErrorStream << fmt::format("\n{} | {}", expectedResultLine, actualResultLine);
                lineIdxIt.advanceExpected();
                lineIdxIt.advanceActual();
                return true;
            }
            populateErrorWithMatchingFields(
                lineComparisonErrorStream, expectedToActualFieldMap, splitExpected, splitActualResult, lineIdxIt);
            return false;
        }
        case FieldMatchResult::ALL_EXISTING_FIELD_MATCHED: {
            populateErrorWithMatchingFields(
                lineComparisonErrorStream, expectedToActualFieldMap, splitExpected, splitActualResult, lineIdxIt);
            return false;
        }
        case FieldMatchResult::AT_LEAST_ONE_FIELD_MISMATCHED: {
            if (expectedResultLine < actualResultLine)
            {
                lineComparisonErrorStream << fmt::format("\n{} | {}", expectedResultLine, std::string(expectedResultLine.size(), '_'));
                lineIdxIt.advanceExpected();
            }
            else
            {
                lineComparisonErrorStream << fmt::format("\n{} | {}", std::string(actualResultLine.size(), '_'), actualResultLine);
                lineIdxIt.advanceActual();
            }
            return false;
        }
    }
    std::unreachable();
}

inline void sortOnFields(std::vector<std::string>& results, const std::vector<size_t>& fieldIdxs)
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

std::stringstream compareResults(
    std::vector<std::string>& formattedExpectedResultLines,
    std::vector<std::string>& formattedActualResultLines,
    const ExpectedToActualFieldMap& expectedToActualFieldMap)
{
    std::stringstream lineComparisonErrorStream;
    /// We allow commas in the result and the expected result. To ensure they are equal we remove them from both.
    /// Additionally, we remove double spaces, as we expect a single space between the fields
    std::ranges::for_each(formattedActualResultLines, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
    std::ranges::for_each(formattedExpectedResultLines, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
    std::ranges::for_each(formattedActualResultLines, NES::Util::removeDoubleSpaces);
    std::ranges::for_each(formattedExpectedResultLines, NES::Util::removeDoubleSpaces);

    sortOnFields(formattedExpectedResultLines, expectedToActualFieldMap.expectedResultsFieldSortIdx);
    sortOnFields(formattedActualResultLines, expectedToActualFieldMap.actualResultsFieldSortIdx);

    bool allResultTuplesMatch = true;
    LineIndexIterator lineIdxIt{formattedExpectedResultLines.size(), formattedActualResultLines.size()};
    while (lineIdxIt.hasNext())
    {
        if (lineIdxIt.hasOnlyExpectedLinesLeft())
        {
            const auto& expectedLine = formattedExpectedResultLines.at(lineIdxIt.getExpected());
            lineComparisonErrorStream << fmt::format("\n{} | {}", expectedLine, std::string(expectedLine.size(), '_'));
            lineIdxIt.advanceExpected();
            allResultTuplesMatch = false;
            continue;
        }
        if (lineIdxIt.hasOnlyActualLinesLeft())
        {
            const auto& actualLine = formattedActualResultLines.at(lineIdxIt.getActual());
            lineComparisonErrorStream << fmt::format("\n{} | {}", std::string(actualLine.size(), '_'), actualLine);
            lineIdxIt.advanceActual();
            allResultTuplesMatch = false;
            continue;
        }
        /// Both sets still have lines check if the lines are equal
        allResultTuplesMatch &= compareLines(
            lineComparisonErrorStream,
            formattedExpectedResultLines.at(lineIdxIt.getExpected()),
            formattedActualResultLines.at(lineIdxIt.getActual()),
            expectedToActualFieldMap,
            lineIdxIt);
    }
    if (allResultTuplesMatch)
    {
        return std::stringstream{};
    }
    return lineComparisonErrorStream;
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
    explicit QueryCheckResult(std::stringstream schemaErrorStream, std::stringstream resultErrorStream)
        : schemaErrorStream(std::move(schemaErrorStream)), resultErrorStream(std::move(resultErrorStream))
    {
        if (not(this->schemaErrorStream.view().empty()) and not(this->resultErrorStream.view().empty()))
        {
            this->type = Type::SCHEMAS_MISMATCH_RESULTS_MISMATCH;
        }
        else if (not(this->schemaErrorStream.view().empty()) and this->resultErrorStream.view().empty())
        {
            this->type = Type::SCHEMAS_MISMATCH_RESULTS_MATCH;
        }
        else if (this->schemaErrorStream.view().empty() and not(this->resultErrorStream.view().empty()))
        {
            this->type = Type::SCHEMAS_MATCH_RESULTS_MISMATCH;
        }
        else if (this->schemaErrorStream.view().empty() and this->resultErrorStream.view().empty())
        {
            this->type = Type::SCHEMAS_MATCH_RESULTS_MATCH;
        }
    }

    Type type;
    std::string queryError{};
    std::stringstream schemaErrorStream{};
    std::stringstream resultErrorStream{};
};

QueryCheckResult checkQuery(const NES::Systest::RunningQuery& runningQuery, const NES::Systest::QueryResultMap& queryResultMap)
{
    /// Get result for running query
    const auto queryResult = loadQueryResult(runningQuery.query);
    if (not queryResult.has_value())
    {
        return QueryCheckResult{fmt::format("Failed to load query result for query: ", runningQuery.query.queryDefinition)};
    }
    auto [actualResultSchema, actualQueryResult] = queryResult.value();

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

    /// Compare the expected and actual result schema and the expected and actual result lines/tuples
    auto expectedToActualResultMap = compareSchemas(runningQuery.query.expectedSinkSchema, actualResultSchema);
    auto resultComparisonErrorStream = compareResults(expectedQueryResult, actualQueryResult, expectedToActualResultMap);

    return QueryCheckResult{std::move(expectedToActualResultMap.schemaErrorStream), std::move(resultComparisonErrorStream)};
}
}

namespace NES::Systest
{

bool compareStringAsTypeWithError(DataType::Type type, const std::string& left, const std::string& right)
{
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
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::VARSIZED:
            return left == right;
        case DataType::Type::FLOAT32:
            return compareStringAsTypeWithError<float>(left, right, EPSILON);
        case DataType::Type::FLOAT64:
            return compareStringAsTypeWithError<double>(left, right, EPSILON);
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting UNDEFINED in result check comparison");
    }
    std::unreachable();
}

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
            return fmt::format("{}{}", ResultMismatchMessage, checkQueryResult.resultErrorStream.str());
        }
        case QueryCheckResult::Type::SCHEMAS_MISMATCH_RESULTS_MATCH: {
            return fmt::format("{}\n\nAll Results match", SchemaMismatchMessage, checkQueryResult.schemaErrorStream.str());
        }
        case QueryCheckResult::Type::SCHEMAS_MISMATCH_RESULTS_MISMATCH: {
            return fmt::format(
                "{}{}{}{}",
                SchemaMismatchMessage,
                checkQueryResult.schemaErrorStream.str(),
                ResultMismatchMessage,
                checkQueryResult.resultErrorStream.str());
        }
    }
    std::unreachable();
}
}
