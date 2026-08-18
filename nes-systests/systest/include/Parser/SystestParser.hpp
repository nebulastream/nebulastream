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
#include <functional>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Model/ConfigurationOverride.hpp>
#include <Model/SystestQueryId.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
using namespace std::literals;

/// Tokens ///
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

enum class TestDataIngestionType : uint8_t
{
    INLINE,
    FILE
};

enum class ResultType : uint8_t
{
    TUPLES,
    VERBATIM
};

/// Assigns query numbers and rejects a test file whose query count and result count do not match.
class QueryIdAssigner
{
    static constexpr SystestQueryId::Underlying INITIAL_QUERY_NUMBER = SystestQueryId::INITIAL;

public:
    explicit QueryIdAssigner() = default;

    [[nodiscard]] SystestQueryId getNextQueryNumber()
    {
        if (currentQueryNumber != currentQueryResultNumber)
        {
            throw SLTUnexpectedToken(
                "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
        }

        return SystestQueryId(currentQueryNumber++);
    }

    [[nodiscard]] SystestQueryId getNextQueryResultNumber()
    {
        if (currentQueryNumber != (currentQueryResultNumber + 1))
        {
            throw SLTUnexpectedToken(
                "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
        }

        return SystestQueryId(currentQueryResultNumber++);
    }

private:
    SystestQueryId::Underlying currentQueryNumber = SystestQueryId::INITIAL;
    SystestQueryId::Underlying currentQueryResultNumber = SystestQueryId::INITIAL;
};

/// Parses a dialect of the sqllogictest format and reports each section that it reads to a callback.
/// A pull parser, following https://www.think-cell.com/assets/en/career/talks/pdf/think-cell_talk_json.pdf
/// A caller registers substitution rules before loading a test file, and callbacks before parsing it.
class SystestParser
{
public:
    struct SubstitutionRule
    {
        std::string keyword;
        /// Rewrites the keyword in place.
        std::function<void(std::string&)> ruleFunction;
    };

    /// Registers a substitution rule, which the parser applies while loading a test file.
    void registerSubstitutionRule(const SubstitutionRule& rule);

    /// Fills the line buffer that the parser reads from, dropping comments and applying the registered substitution rules.
    /// Loading replaces whatever was loaded before.
    /// The caller supplies the text of a test file, so the parser never opens a file.
    void loadString(const std::string& str);

    struct ErrorExpectation
    {
        ErrorCode code;
        std::optional<std::string> message;
        bool operator==(const ErrorExpectation& other) const = default;
    };

    using QueryCallback = std::function<void(std::string, SystestQueryId, bool)>;
    using ExplainQueryCallback = std::function<void(std::string, SystestQueryId)>;
    using ResultTuplesCallback = std::function<void(std::vector<std::string>&&, SystestQueryId correspondingQueryId)>;
    using ErrorExpectationCallback = std::function<void(const ErrorExpectation&, SystestQueryId correspondingQueryId)>;
    using DifferentialQueryBlockCallback
        = std::function<void(std::string, std::string, SystestQueryId correspondingQueryId, SystestQueryId diffQueryId)>;
    using CreateCallback = std::function<void(std::string, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>)>;
    using ConfigurationCallback = std::function<void(const std::vector<ConfigurationOverride>&)>;
    using GlobalConfigurationCallback = std::function<void(const std::vector<ConfigurationOverride>&)>;

    /// Registers the callback that the parser reports the matching section to.
    void registerOnQueryCallback(QueryCallback callback);
    void registerOnExplainQueryCallback(ExplainQueryCallback callback);
    void registerOnResultTuplesCallback(ResultTuplesCallback callback);
    void registerOnErrorExpectationCallback(ErrorExpectationCallback callback);
    void registerOnCreateCallback(CreateCallback callback);
    void registerOnDifferentialQueryBlockCallback(DifferentialQueryBlockCallback callback);
    void registerOnConfigurationCallback(ConfigurationCallback callback);
    void registerOnGlobalConfigurationCallback(GlobalConfigurationCallback callback);

    void parse();

private:
    /// Parsing utils ///
    [[nodiscard]] static std::optional<TokenType> getTokenIfValid(const std::string& line);
    /// Reads the next token and returns its type.
    [[nodiscard]] std::optional<TokenType> getNextToken();
    /// Advances to the next token, and returns false at the end of the file.
    [[nodiscard]] bool moveToNextToken();
    /// Returns the next token without consuming it.
    [[nodiscard]] std::optional<TokenType> peekToken() const;

    /// Applies the registered substitution rules to one line.
    void applySubstitutionRules(std::string& line);

    [[nodiscard]] std::vector<std::string> expectTuples(bool ignoreFirst);
    [[nodiscard]] std::vector<std::string> expectVerbatimResultLines();
    [[nodiscard]] std::string expectQuery();
    [[nodiscard]] std::pair<std::string, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>> expectCreateStatement();
    [[nodiscard]] std::string expectQuery(const std::unordered_set<TokenType>& stopTokens);
    [[nodiscard]] std::pair<std::string, std::string> expectDifferentialBlock();
    [[nodiscard]] ErrorExpectation expectError() const;
    [[nodiscard]] std::vector<ConfigurationOverride> expectConfiguration();
    [[nodiscard]] std::vector<ConfigurationOverride> expectGlobalConfiguration();

    std::vector<SubstitutionRule> substitutionRules;
    QueryCallback onQueryCallback;
    ExplainQueryCallback onExplainQueryCallback;
    ResultTuplesCallback onResultTuplesCallback;
    ErrorExpectationCallback onErrorExpectationCallback;
    CreateCallback onCreateCallback;
    DifferentialQueryBlockCallback onDifferentialQueryBlockCallback;
    ConfigurationCallback onConfigurationCallback;
    GlobalConfigurationCallback onGlobalConfigurationCallback;

    std::optional<std::string> lastParsedQuery;
    std::optional<SystestQueryId> lastParsedQueryId;
    ResultType expectedResultType = ResultType::TUPLES;
    bool firstToken = true;
    bool shouldRevisitCurrentLine = false;
    size_t currentLine = 0;
    std::vector<std::string> lines;
};
}
