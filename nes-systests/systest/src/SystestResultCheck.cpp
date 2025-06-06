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

std::pair<std::vector<std::string>, std::vector<std::string>>
formatResultLines(const std::vector<std::string>& expectedQueryResultLines, const std::vector<std::string>& actualQueryResultLines)
{
    auto formattedExpectedResultLines = expectedQueryResultLines;
    auto formattedActualResultLines = actualQueryResultLines;
    /// We allow commas in the result and the expected result. To ensure they are equal we remove them from both.
    /// Additionally, we remove double spaces, as we expect a single space between the fields
    std::ranges::for_each(formattedActualResultLines, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
    std::ranges::for_each(formattedExpectedResultLines, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
    std::ranges::for_each(formattedActualResultLines, NES::Util::removeDoubleSpaces);
    std::ranges::for_each(formattedExpectedResultLines, NES::Util::removeDoubleSpaces);

    std::ranges::sort(formattedActualResultLines);
    std::ranges::sort(formattedExpectedResultLines);

    return std::make_pair(formattedExpectedResultLines, formattedActualResultLines);
}

bool hasEqualFields(
    const NES::Schema& expectedResultSchema,
    const std::vector<std::string>& splitExpected,
    const std::vector<std::string>& splitActualResult)
{
    bool isEqual = true;
    for (const auto& [fieldIdx, expectedField] : splitExpected | NES::views::enumerate)
    {
        if (expectedResultSchema.getFieldAt(fieldIdx).dataType.isType(NES::DataType::Type::FLOAT32))
        {
            if (not NES::Systest::compareStringAsTypeWithError<float>(expectedField, splitActualResult.at(fieldIdx)))
            {
                isEqual = false;
                break;
            }
        }
        else if (expectedResultSchema.getFieldAt(fieldIdx).dataType.isType(NES::DataType::Type::FLOAT64))
        {
            if (not NES::Systest::compareStringAsTypeWithError<double>(expectedField, splitActualResult.at(fieldIdx)))
            {
                isEqual = false;
                break;
            }
        }
        else
        {
            if (expectedField != splitActualResult.at(fieldIdx))
            {
                isEqual = false;
                break;
            }
        }
    }
    return isEqual;
}

std::string createResultTupleErrorStream(
    const std::vector<std::string>& expectedQueryResultLines,
    const std::vector<std::string>& actualQueryResultLines,
    const NES::Schema& expectedResultSchema)
{
    std::stringstream localErrorStream;
    if (expectedQueryResultLines.size() == actualQueryResultLines.size())
    {
        if (/* isResultsEmpty */ expectedQueryResultLines.empty() and actualQueryResultLines.empty())
        {
            return "";
        }

        size_t numberOfMismatches = 0;
        localErrorStream << "\nResult Mismatch(Tuple Mismatch)\n";
        localErrorStream << "Expected Tuples | Actual Result Tuples";

        std::ranges::for_each(
            std::views::zip(expectedQueryResultLines, actualQueryResultLines),
            [&localErrorStream, &numberOfMismatches, &expectedResultSchema](const auto& pair)
            {
                const auto& expectedLine = std::get<0>(pair);
                const auto& actualResultLine = std::get<1>(pair);
                auto splitExpected = NES::Util::splitWithStringDelimiter<std::string>(expectedLine, " ");
                auto splitActualResult = NES::Util::splitWithStringDelimiter<std::string>(actualResultLine, " ");

                if (splitExpected.size() != splitActualResult.size())
                {
                    fmt::print(localErrorStream, "\nField Count Missmatch: {} | actualResultLine", expectedLine, actualResultLine);
                    ++numberOfMismatches;
                    return;
                }

                if (hasEqualFields(expectedResultSchema, splitExpected, splitActualResult))
                {
                    fmt::print(localErrorStream, "\n          {} | {}", expectedLine, actualResultLine);
                }
                else
                {
                    localErrorStream << fmt::format("\nField Mismatch: {} | {}", expectedLine, actualResultLine);
                    ++numberOfMismatches;
                }
            });
        if (numberOfMismatches != 0)
        {
            return localErrorStream.str();
        }
        return "";
    }
    /// Sizes mismatch, log all tuples as errors
    localErrorStream << "\nResult Mismatch(Size Mismatch)\n";
    localErrorStream << "Expected Tuples | Actual Result Tuples";

    /// Determine the larger of the two result line vectors. When the smaller vector runs out of lines, log ' (Missing) ' instead of a tuple
    const size_t maxNumberOfResultTuples = std::max(expectedQueryResultLines.size(), actualQueryResultLines.size());
    const auto logLine = [](const auto& lines, size_t lineIdx) { return lineIdx < lines.size() ? lines.at(lineIdx) : " (Missing) "; };
    for (size_t lineIdx = 0; lineIdx < maxNumberOfResultTuples; ++lineIdx)
    {
        localErrorStream << fmt::format("\n{} | {}", logLine(expectedQueryResultLines, lineIdx), logLine(actualQueryResultLines, lineIdx));
    }
    return localErrorStream.str();
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

    std::stringstream errorMessages;
    uint64_t seenResultTupleSections = 0;
    NES_INFO(
        "seenResultTupleSections: {}, runningQuery.query.queryIdInFile: {}", seenResultTupleSections, runningQuery.query.queryIdInFile);
    /// Get query result
    const auto queryResult = loadQueryResult(runningQuery.query);
    if (not queryResult.has_value())
    {
        errorMessages << "Failed to load query result for query: " << runningQuery.query.queryDefinition << "\n";
        return std::nullopt;
    }
    auto [schemaResult, actualQueryResultLines] = queryResult.value();

    /// Check if the expected result is empty and if this is the case, the query result should be empty as well
    const auto currentQuery = queryResultMap.find(runningQuery.query.resultFile());
    INVARIANT(
        currentQuery != queryResultMap.end(),
        "Could not find query {}:{} in result map.",
        runningQuery.query.sqlLogicTestFile,
        runningQuery.query.queryIdInFile);

    /// Get formatted and sorted result lines
    const auto [formattedExpectedResultLines, formattedActualResultLines] = formatResultLines(currentQuery->second, actualQueryResultLines);

    /// Check if schemas are equal. If not populate the error stream
    if (/* hasMatchingSchema */ runningQuery.query.expectedSinkSchema != schemaResult)
    {
        errorMessages << fmt::format(
            "\nSchema mismatch\nExpected Test Schema({})\nActual Result Schema({})\n",
            fmt::join(runningQuery.query.expectedSinkSchema, ", "),
            fmt::join(schemaResult, ", "));

        /// We logged the mismatching schema and all tupes (sorted), there are no more checks to do, return
        if (const auto resulTupleErrorStream
            = createResultTupleErrorStream(formattedExpectedResultLines, formattedActualResultLines, runningQuery.query.expectedSinkSchema);
            not resulTupleErrorStream.empty())
        {
            errorMessages << resulTupleErrorStream;
            return errorMessages.str();
        }
        errorMessages << "\nAll Tuples Match";
        return errorMessages.str();
    }

    /// The schemas are equal, compare the result tuples and if not equal, populate the error stream
    if (const auto resulTupleErrorStream
        = createResultTupleErrorStream(formattedExpectedResultLines, formattedActualResultLines, runningQuery.query.expectedSinkSchema);
        not resulTupleErrorStream.empty())
    {
        errorMessages << resulTupleErrorStream;
        return errorMessages.str();
    }
    return std::nullopt;
}

}
