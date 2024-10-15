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

#include <SystestState.hpp>

namespace NES::Systest
{

bool getResultOfQuery(const std::string& testName, uint64_t queryNr, std::vector<std::string>& resultData)
{
    std::string resultFileName = PATH_TO_BINARY_DIR "/nes-systests/result/" + testName + std::to_string(queryNr) + ".csv";
    std::ifstream resultFile(resultFileName);
    if (!resultFile)
    {
        NES_FATAL_ERROR("Failed to open result file: {}", resultFileName);
        return false;
    }

    std::string line;
    std::regex headerRegex(R"(.*\$.*:.*)");
    while (std::getline(resultFile, line))
    {
        /// Skip the header
        if (std::regex_match(line, headerRegex))
        {
            continue;
        }
        resultData.push_back(line);
    }
    return true;
}

bool checkResult(const Query& query)
{
    SLTParser::SLTParser parser{};
    if (!parser.loadFile(query.testFile.value().get().path))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", query.testFile.value().get().path);
        return false;
    }

    bool same = true;
    std::string printMessages;
    std::string errorMessages;
    uint64_t seenResultTupleSections = 0;
    uint64_t seenQuerySections = 0;

    constexpr bool printOnlyDifferences = true;
    parser.registerOnResultTuplesCallback(
        [&seenResultTupleSections, &errorMessages, &query, &same](SLTParser::SLTParser::ResultTuples&& resultLines)
        {
            /// Result section matches with query --> we found the expected result to check for
            if (seenResultTupleSections == query.nrInFile)
            {
                /// 1. Get query result
                std::vector<std::string> queryResult;
                if (!getResultOfQuery(query.name, seenResultTupleSections, queryResult))
                {
                    same = false;
                    return;
                }

                /// 2. We allow commas in the result and the expected result. To ensure they are equal we remove them from both
                std::for_each(queryResult.begin(), queryResult.end(), [](std::string& s) { std::replace(s.begin(), s.end(), ',', ' '); });
                std::for_each(resultLines.begin(), resultLines.end(), [](std::string& s) { std::replace(s.begin(), s.end(), ',', ' '); });

                /// 3. Store lines with line ids
                std::vector<std::pair<int, std::string>> originalResultLines;
                std::vector<std::pair<int, std::string>> originalQueryResult;

                originalResultLines.reserve(resultLines.size());
                for (size_t i = 0; i < resultLines.size(); ++i)
                {
                    originalResultLines.emplace_back(i + 1, resultLines[i]); /// store with 1-based index
                }

                originalQueryResult.reserve(queryResult.size());
                for (size_t i = 0; i < queryResult.size(); ++i)
                {
                    originalQueryResult.emplace_back(i + 1, queryResult[i]); /// store with 1-based index
                }

                /// 4. Check if line sizes match
                if (queryResult.size() != resultLines.size())
                {
                    errorMessages += "Result does not match for query: " + std::to_string(seenResultTupleSections) + "\n";
                    errorMessages += "Result size does not match: expected " + std::to_string(resultLines.size()) + ", got "
                        + std::to_string(queryResult.size()) + "\n";
                    same = false;
                }

                /// 5. Check if content match
                std::sort(queryResult.begin(), queryResult.end());
                std::sort(resultLines.begin(), resultLines.end());

                if (!std::equal(queryResult.begin(), queryResult.end(), resultLines.begin()))
                {
                    /// 6. Build error message
                    errorMessages += "Result does not match for query " + std::to_string(seenResultTupleSections) + ":\n";
                    errorMessages += "[#Line: Expected Result | #Line: Query Result]\n";

                    /// Maximum width of "Expected (Line #)"
                    size_t maxExpectedWidth = 0;
                    for (const auto& line : originalResultLines)
                    {
                        size_t currentWidth = std::to_string(line.first).length() + 2 + line.second.length();
                        maxExpectedWidth = std::max(currentWidth, maxExpectedWidth);
                    }

                    auto maxSize = std::max(originalResultLines.size(), originalQueryResult.size());
                    for (size_t i = 0; i < maxSize; ++i)
                    {
                        std::string expectedStr;
                        if (i < originalResultLines.size())
                        {
                            expectedStr = std::to_string(originalResultLines[i].first) + ": " + originalResultLines[i].second;
                        }
                        else
                        {
                            expectedStr = "(missing)";
                        }

                        std::string gotStr;
                        if (i < originalQueryResult.size())
                        {
                            gotStr = std::to_string(originalQueryResult[i].first) + ": " + originalQueryResult[i].second;
                        }
                        else
                        {
                            gotStr = "(missing)";
                        }

                        /// Check if they are different or if we are not filtering by differences
                        bool const areDifferent
                            = (i >= originalResultLines.size() || i >= originalQueryResult.size()
                               || originalResultLines[i].second != originalQueryResult[i].second);

                        if (areDifferent || !printOnlyDifferences)
                        {
                            /// Align the expected string by padding it to the maximum width
                            errorMessages += expectedStr;
                            errorMessages += std::string(maxExpectedWidth - expectedStr.length(), ' ');
                            errorMessages += " | ";
                            errorMessages += gotStr;
                            errorMessages += "\n";
                        }
                    }

                    same = false;
                }
            }
            seenResultTupleSections++;
        });

    parser.registerOnQueryCallback(
        [&seenQuerySections, &query, &printMessages](const std::string& queryStr)
        {
            if (seenQuerySections == query.nrInFile)
            {
                printMessages = queryStr;
            }
            seenQuerySections++;
        });

    try
    {
        parser.parse();
    }
    catch (const Exception&)
    {
        tryLogCurrentException();
        return false;
    }

    /// spd logger cannot handle multiline prints with proper color and pattern. And as this is only for test runs we use stdout here.
    std::cout << "==============================================================" << '\n';
    std::cout << printMessages << '\n';
    std::cout << "==============================================================" << std::endl;
    if (!same)
    {
        std::cerr << errorMessages << std::endl;
    }

    return same;
}


}