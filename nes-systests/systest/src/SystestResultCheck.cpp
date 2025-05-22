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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/base.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/Char.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES::Systest
{

SystestParser::Schema parseFieldNames(const std::string_view fieldNamesRawLine)
{
    /// Assumes the field and type to be similar to
    /// window$val_i8_i8:INT32, window$val_i8_i8_plus_1:INT16
    SystestParser::Schema fields;
    for (auto field : std::ranges::split_view(fieldNamesRawLine, ',')
             | std::views::transform([](auto splittedNameAndType)
                                     { return std::string_view(splittedNameAndType.begin(), splittedNameAndType.end()); })
             | std::views::filter([](const auto& stringViewSplitted) { return !stringViewSplitted.empty(); }))
    {
        /// At this point, we have a field and tpye separated by a colon, e.g., "window$val_i8_i8:INT32"
        /// We need to split the fieldName and type by the colon, store the field name and type in a vector.
        /// After that, we can trim the field name and type and store it in the fields vector.
        /// "window$val_i8_i8:INT32 " -> ["window$val_i8_i8", "INT32 "] -> {INT32, "window$val_i8_i8"}
        std::vector<std::string_view> fieldAndType;
        for (auto&& subrange : std::ranges::split_view(field, ':'))
        {
            fieldAndType.emplace_back(subrange.begin(), subrange.end());
        }
        const auto nameTrimmed = Util::trimWhiteSpaces(fieldAndType[0]);
        const auto typeTrimmed = Util::trimWhiteSpaces(fieldAndType[1]);
        std::shared_ptr<DataType> dataType;
        if (auto type = magic_enum::enum_cast<BasicType>(typeTrimmed); type.has_value())
        {
            dataType = DataTypeProvider::provideBasicType(type.value());
        }
        else if (NES::Util::toLowerCase(typeTrimmed) == "varsized")
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::VARSIZED);
        }
        else
        {
            throw SLTUnexpectedToken("Unknown basic type: {}", typeTrimmed);
        }
        fields.emplace_back(dataType, std::string(nameTrimmed));
    }
    return fields;
}

std::pair<std::vector<MapFieldNameToValue>, std::vector<MapFieldNameToValue>> parseResultTuples(
    const SystestParser::Schema& schemaExpected,
    const SystestParser::Schema& queryResultFields,
    const SystestParser::ResultTuples& expectedResultLines,
    const SystestParser::ResultTuples& queryResultLines)
{
    /// Store lines and the corresponding field values. We need a hashmap, as the order of the fields can be different
    std::vector<MapFieldNameToValue> queryResultExpected;
    std::vector<MapFieldNameToValue> queryResultActual;

    /// Lambda for creating the field values from a line with the schema. For example, "1 2 3" with schema "field1:INT32, field2:FLOAT32, field3:INT64"
    /// would be converted to {"field1": {INT32, "1"}, "field2": {FLOAT32, "2"}, "field3": {INT64, "3"}}
    auto createFieldValues = [](const SystestParser::Schema& schema, const std::string& line) -> MapFieldNameToValue
    {
        MapFieldNameToValue fieldValues;
        std::stringstream lineAsStream(line);
        std::string value;
        for (const auto& [type, name] : schema)
        {
            std::getline(lineAsStream, value, ' ');
            fieldValues[name] = {.type = type, .valueAsString = value};
        }
        return fieldValues;
    };

    /// Parse the expected result into a hashmap as the order of the fields can be different
    for (const auto& line : expectedResultLines)
    {
        const auto fieldValues = createFieldValues(schemaExpected, line);
        queryResultExpected.emplace_back(fieldValues); /// store with 1-based index
    }

    /// Parse the query result into a hashmap as the order of the fields can be different
    for (const auto& line : queryResultLines)
    {
        const auto fieldValues = createFieldValues(queryResultFields, line);
        queryResultActual.emplace_back(fieldValues); /// store with 1-based index
    }
    return std::pair{queryResultExpected, queryResultActual};
}

static void populateErrorStream(
    std::stringstream& errorMessages,
    const SystestParser::Schema& schemaExpected,
    const std::vector<MapFieldNameToValue>& queryResultExpected,
    const std::vector<MapFieldNameToValue>& queryResultActual,
    const uint64_t seenResultTupleSections)
{
    errorMessages << "Result does not match for query " + std::to_string(seenResultTupleSections) + ":\n";
    errorMessages << "[#Line: Expected Result | #Line: Query Result]\n";

    /// Writing the schema fields
    for (const auto& [_, name] : schemaExpected)
    {
        errorMessages << name + " ";
    }
    errorMessages << "\n";

    const auto maxSize = std::max(queryResultExpected.size(), queryResultActual.size());
    for (size_t i = 0; i < maxSize; ++i)
    {
        /// Check if they are different or if we are not filtering by differences
        const bool areDifferent
            = (i >= queryResultExpected.size() || i >= queryResultActual.size() || queryResultExpected[i] != queryResultActual[i]);

        if (areDifferent)
        {
            std::stringstream expectedStr;
            if (i < queryResultExpected.size())
            {
                const auto& expectedMap = queryResultExpected[i];
                expectedStr << std::to_string(i) << ": ";
                for (const auto& [_, name] : schemaExpected)
                {
                    if (not expectedMap.contains(name))
                    {
                        expectedStr << "(missing) ";
                        continue;
                    }
                    expectedStr << expectedMap.at(name).valueAsString + " ";
                }
            }

            std::stringstream gotStr;
            if (i < queryResultActual.size())
            {
                gotStr << std::to_string(i) << ": ";
                const auto& queryResultMap = queryResultActual[i];
                for (const auto& [_, name] : schemaExpected)
                {
                    if (not queryResultMap.contains(name))
                    {
                        gotStr << "(missing) ";
                        continue;
                    }
                    gotStr << queryResultMap.at(name).valueAsString + " ";
                }
            }

            errorMessages << expectedStr.rdbuf();
            errorMessages << " | ";
            errorMessages << gotStr.rdbuf();
            errorMessages << "\n";
        }
    }
}

std::optional<QueryResult> loadQueryResult(const Query& query)
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

std::optional<std::string> checkResult(const RunningQuery& runningQuery)
{
    SystestParser parser{};
    if (!parser.loadFile(runningQuery.query.sqlLogicTestFile))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", runningQuery.query.sqlLogicTestFile);
        return "Failed to parse test file: " + runningQuery.query.sqlLogicTestFile.string() + "\n";
    }

    std::stringstream errorMessages;
    uint64_t seenResultTupleSections = 0;
    parser.registerOnResultTuplesCallback(
        [&seenResultTupleSections, &errorMessages, &runningQuery](SystestParser::ResultTuples&& expectedResultLines)
        {
            NES_INFO(
                "seenResultTupleSections: {}, runningQuery.query.queryIdInFile: {}",
                seenResultTupleSections,
                runningQuery.query.queryIdInFile);
            /// Result section matches with query --> we found the expected result to check for
            if (seenResultTupleSections++ == runningQuery.query.queryIdInFile)
            {
                /// 1. Get query result
                const auto opt = loadQueryResult(runningQuery.query);
                if (!opt)
                {
                    errorMessages << "Failed to load query result for query: " << runningQuery.query.queryDefinition << "\n";
                    return;
                }
                auto [schemaResult, queryResultLines] = opt.value();

                /// Check if the expected result is empty and if this is the case, the query result should be empty as well
                if (expectedResultLines.empty())
                {
                    if (not queryResultLines.empty())
                    {
                        errorMessages << "Result does not match for query: " << std::to_string(seenResultTupleSections) << "\n";
                        errorMessages << "Expected result is empty, but query result is not\n";
                        return;
                    }
                    return;
                }

                /// 2. We allow commas in the result and the expected result. To ensure they are equal we remove them from both.
                /// Additionally, we remove double spaces, as we expect a single space between the fields
                std::ranges::for_each(queryResultLines, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
                std::ranges::for_each(expectedResultLines, [](std::string& line) { std::ranges::replace(line, ',', ' '); });
                std::ranges::for_each(queryResultLines, Util::removeDoubleSpaces);
                std::ranges::for_each(expectedResultLines, Util::removeDoubleSpaces);


                /// 3. Parse the expected result into a hashmap as the order of the fields can be different
                auto [queryResultExpected, queryResultActual]
                    = parseResultTuples(runningQuery.query.expectedSinkSchema, schemaResult, expectedResultLines, queryResultLines);


                /// 4. Check if there exist the same amount of lines
                if (queryResultExpected.size() != queryResultActual.size())
                {
                    errorMessages << "Result does not match for query: " << std::to_string(seenResultTupleSections) << "\n";
                    errorMessages << "Result size does not match: expected " << std::to_string(queryResultExpected.size());
                    errorMessages << ", got " << std::to_string(queryResultActual.size()) << "\n";
                }

                /// 5. Check if for all fields of the result expected, we can find one equal field in the actual result
                /// Stores if we have found a result for the expected result idx
                std::unordered_set<uint64_t> idxExpectedToFound;
                uint64_t idxExpected = 0;
                for (const auto& expected : queryResultExpected)
                {
                    if (idxExpected >= queryResultActual.size())
                    {
                        errorMessages << "Result does not match for query: " << std::to_string(seenResultTupleSections) << "\n";
                        errorMessages << "Expected result has more lines than the actual result\n";
                        break;
                    }

                    /// If we have found an idx for the expected result, we can skip it
                    if (idxExpectedToFound.contains(idxExpected))
                    {
                        continue;
                    }

                    for (const auto& actual : queryResultActual)
                    {
                        if (expected == actual)
                        {
                            idxExpectedToFound.emplace(idxExpected);
                            break;
                        }
                    }

                    ++idxExpected;
                }

                /// 6. Build error message if we have not found a result for every expected result
                if (idxExpectedToFound.size() != queryResultExpected.size())
                {
                    populateErrorStream(
                        errorMessages,
                        runningQuery.query.expectedSinkSchema,
                        queryResultExpected,
                        queryResultActual,
                        seenResultTupleSections);
                }
            }
        });

    try
    {
        parser.parse();
    }
    catch (const Exception& e)
    {
        tryLogCurrentException();
        return "Exception occurred: " + std::string(e.what());
    }

    if (errorMessages.str().empty())
    {
        return std::nullopt;
    }
    return errorMessages.str();
}

bool operator!=(const FieldResult& left, const FieldResult& right)
{
    /// Check if the type is equal
    if (*left.type != *right.type)
    {
        return true;
    }

    /// Check if the value is equal by casting it to the correct type and comparing it (we allow a small delta)
    if (NES::Util::instanceOf<VariableSizedDataType>(left.type) || NES::Util::instanceOf<Char>(left.type))
    {
        return left.valueAsString != right.valueAsString;
    }
    if (NES::Util::as_if<Boolean>(left.type))
    {
        return not compareStringAsTypeWithError<bool>(left.valueAsString, right.valueAsString);
    }
    if (NES::Util::as_if<Integer>(left.type))
    {
        return not compareStringAsTypeWithError<int64_t>(left.valueAsString, right.valueAsString);
    }
    if (NES::Util::as_if<Float>(left.type))
    {
        return not compareStringAsTypeWithError<double>(left.valueAsString, right.valueAsString);
    }

    throw UnknownPhysicalType("Unknown type {}", left.type->toString());
}

bool operator==(const MapFieldNameToValue& left, const MapFieldNameToValue& right)
{
    /// Check if the size is the same
    if (left.size() != right.size())
    {
        NES_ERROR("Size of fields does not match: {} != {}", left.size(), right.size());
        return false;
    }

    /// Check that all fields are the same
    return std::ranges::all_of(
        left,
        [&right](const auto& pair) -> bool
        {
            const auto& [name, field] = pair;
            if (not right.contains(name))
            {
                NES_WARNING("Field {} does not exist in other schema", name);
                return false;
            }

            if (field != right.at(name))
            {
                NES_TRACE(
                    "Field {} does not match {} ({}) != {} ({})",
                    name,
                    field.valueAsString,
                    field.type->toString(),
                    right.at(name).valueAsString,
                    right.at(name).type->toString());
                return false;
            }
            return true;
        });
}

bool operator!=(const MapFieldNameToValue& left, const MapFieldNameToValue& right)
{
    return not(left == right);
}
}
