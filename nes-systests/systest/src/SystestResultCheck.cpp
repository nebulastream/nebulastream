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

namespace NES::Systest
{
std::optional<std::vector<std::string>> loadQueryResult(const Query& query)
{
    std::vector<std::string> resultData;
    std::ifstream resultFile(query.resultFile());
    if (!resultFile)
    {
        NES_FATAL_ERROR("Failed to open result file: {}", query.resultFile());
        return std::nullopt;
    }

    std::string firstLine;
    if (!std::getline(resultFile, firstLine))
    {
        return resultData;
    }
    if (firstLine.find("$") == std::string::npos || firstLine.find(":") == std::string::npos)
    {
        resultData.push_back(firstLine);
    }

    while (std::getline(resultFile, firstLine))
    {
        resultData.push_back(firstLine);
    }
    return resultData;
}

/// TODO #373: refactor result checking to be type save
std::optional<std::string> checkResult(const Query& query)
{
    SystestParser parser{};
    if (!parser.loadFile(query.sqlLogicTestFile))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", query.sqlLogicTestFile);
        return "Failed to parse test file: " + query.sqlLogicTestFile.string() + "\n";
    }

    std::stringstream errorMessages;
    uint64_t seenResultTupleSections = 0;

    constexpr bool printOnlyDifferences = true;
    parser.registerOnResultTuplesCallback(
        [&seenResultTupleSections, &errorMessages, &query](SystestParser::SystestParser::ResultTuples&& resultLines)
        {
            /// Result section matches with query --> we found the expected result to check for
            if (seenResultTupleSections == query.queryIdInFile)
            {
                /// 1. Get query result
                const auto opt = loadQueryResult(query);
                if (!opt)
                {
                    errorMessages << "Failed to load query result for query: " << query.queryDefinition << "\n";
                    return;
                }
                auto queryResult = opt.value();

                /// 2. We allow commas in the result and the expected result. To ensure they are equal we remove them from both
                std::ranges::for_each(queryResult, [](std::string& s) { std::ranges::replace(s, ',', ' '); });
                std::ranges::for_each(resultLines, [](std::string& s) { std::ranges::replace(s, ',', ' '); });

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
                    errorMessages << "Result does not match for query: " << std::to_string(seenResultTupleSections) << "\n";
                    errorMessages << "Result size does not match: expected " << std::to_string(resultLines.size());
                    errorMessages << ", got " << std::to_string(queryResult.size()) << "\n";
                }

                /// 5. Check if content match
                std::ranges::sort(queryResult);
                std::ranges::sort(resultLines);

                if (!std::equal(queryResult.begin(), queryResult.end(), resultLines.begin()))
                {
                    /// 6. Build error message
                    errorMessages << "Result does not match for query " << std::to_string(seenResultTupleSections) << ":\n";
                    errorMessages << "[#Line: Expected Result | #Line: Query Result]\n";

                    /// Maximum width of "Expected (Line #)"
                    size_t maxExpectedWidth = 0;
                    for (const auto& [lineNumber, line] : originalResultLines)
                    {
                        size_t currentWidth = std::to_string(lineNumber).length() + 2 + line.length();
                        maxExpectedWidth = std::max(currentWidth, maxExpectedWidth);
                    }

                    const auto maxSize = std::max(originalResultLines.size(), originalQueryResult.size());
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
                            errorMessages << fmt::format(
                                "{}{} | {}\n", expectedStr, std::string(maxExpectedWidth - expectedStr.length(), ' '), gotStr);
                        }
                    }
                }
            }
            seenResultTupleSections++;
        });

    try
    {
        parser.parse();
    }
    catch (const Exception&)
    {
        tryLogCurrentException();
        return "Exception occurred";
    }

    if (errorMessages.str().empty())
    {
        return std::nullopt;
    }
    return errorMessages.str();
}

}
