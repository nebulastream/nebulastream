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

#include <cstddef>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <SystestQueryModel.hpp>

namespace NES::Systest
{

enum class TokenType : uint8_t
{
    QUERY,
    EXPLAIN,
    CREATE,
    RESULT_DELIMITER,
    ERROR_EXPECTATION,
    CONFIGURATION,
    GLOBAL_CONFIGURATION,
    DIFFERENTIAL,
    SEQUENTIAL_EXECUTION,
};

enum class ResultType : uint8_t
{
    TUPLES,
    VERBATIM
};

class SystestQueryIdAssigner
{
    static constexpr SystestQueryId::Underlying INITIAL_QUERY_NUMBER = SystestQueryId::INITIAL;

public:
    [[nodiscard]] SystestQueryId getNextQueryNumber();
    [[nodiscard]] SystestQueryId getNextQueryResultNumber();
    void verifyComplete() const;

private:
    SystestQueryId::Underlying currentQueryNumber = INITIAL_QUERY_NUMBER;
    SystestQueryId::Underlying currentQueryResultNumber = INITIAL_QUERY_NUMBER;
};

class SystestParser
{
public:
    struct SubstitutionRule
    {
        std::string keyword;
        std::function<void(std::string&)> ruleFunction;
    };

    void registerSubstitutionRule(const SubstitutionRule& rule);

    [[nodiscard]] bool loadFile(const std::filesystem::path& filePath);
    [[nodiscard]] bool loadFile(const std::filesystem::path& filePath, const std::filesystem::path& relativeTestFile);
    [[nodiscard]] bool loadString(const std::string& str);
    [[nodiscard]] ParsedTestFile parse();

private:
    [[nodiscard]] static std::optional<TokenType> getTokenIfValid(const std::string& line);
    [[nodiscard]] std::optional<TokenType> getNextToken();
    [[nodiscard]] bool moveToNextToken();
    [[nodiscard]] std::optional<TokenType> peekToken() const;

    void applySubstitutionRules(std::string& line);

    [[nodiscard]] std::vector<std::string> expectTuples(bool ignoreFirst);
    [[nodiscard]] std::vector<std::string> expectVerbatimResultLines();
    [[nodiscard]] std::pair<std::string, std::optional<SourceDataSpec>> expectCreateStatement();
    [[nodiscard]] std::string expectQuery(const std::unordered_set<TokenType>& stopTokens);
    [[nodiscard]] std::pair<std::string, std::string> expectDifferentialBlock();
    [[nodiscard]] ErrorExpectation expectError() const;
    [[nodiscard]] ConfigurationDirective expectConfiguration(bool global) const;

    std::vector<SubstitutionRule> substitutionRules;
    std::optional<std::string> lastParsedQuery;
    std::optional<SystestQueryId> lastParsedQueryId;
    ResultType expectedResultType = ResultType::TUPLES;
    bool firstToken = true;
    bool shouldRevisitCurrentLine = false;
    size_t currentLine = 0;
    std::vector<std::string> lines;
    std::vector<size_t> sourceLineNumbers;
    std::filesystem::path sourceFile;
    std::filesystem::path relativeTestFile;
};

}
