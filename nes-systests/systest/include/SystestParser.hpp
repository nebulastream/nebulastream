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
#include <vector>
#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
using namespace std::literals;

/// Tokens ///
enum class TokenType : uint8_t
{
    INVALID,
    CSV_SOURCE,
    SLT_SOURCE,
    SINK,
    QUERY,
    RESULT_DELIMITER,
    ERROR_EXPECTATION,
};

/// This is a parser for a dialect of the sqllogictest format. We follow a pull-based parser design as proposed in:
/// https://www.think-cell.com/assets/en/career/talks/pdf/think-cell_talk_json.pdf
///
/// NOTE: register substitution rules before calling `loadFile`
/// NOTE: register callbacks before calling `parse`
class SystestParser
{
public:
    struct SubstitutionRule
    {
        std::string keyword;
        /// Takes the keyword by reference and modifies it according to the rule
        std::function<void(std::string&)> ruleFunction;
    };
    /// Register a substitution rule to be applied to the file before parsing
    void registerSubstitutionRule(const SubstitutionRule& rule);

    /// Loading overrides existing parse content
    [[nodiscard]] bool loadFile(const std::filesystem::path& filePath);
    [[nodiscard]] bool loadString(const std::string& str);

    using SystestSchema = std::vector<SystestField>;
    /// Type definitions ///
    struct CSVSource
    {
        std::string name;
        SystestSchema fields;
        std::string csvFilePath;
        bool operator==(const CSVSource& other) const = default;
    };

    struct SystestLogicalSource
    {
        std::string name;
        SystestSchema fields;
        std::vector<std::string> tuples;
        bool operator==(const SystestLogicalSource& other) const = default;
    };

    struct SystestSink
    {
        std::string name;
        SystestSchema fields;
        bool operator==(const SystestSink& other) const = default;
    };

    struct ErrorExpectation
    {
        ErrorCode code;
        std::optional<std::string> message;
        bool operator==(const ErrorExpectation& other) const = default;
    };

    using QueryCallback = std::function<void(std::string, SystestQueryId)>;
    using ResultTuplesCallback = std::function<void(std::vector<std::string>&&, SystestQueryId correspondingQueryId)>;
    using SystestLogicalSourceCallback = std::function<void(const SystestLogicalSource&)>;
    using CSVSourceCallback = std::function<void(CSVSource&&)>;
    using SystestSinkCallback = std::function<void(SystestSink&&)>;
    using ErrorExpectationCallback = std::function<void(const ErrorExpectation&)>;

    /// Register callbacks to be called when the respective section is parsed
    void registerOnQueryCallback(QueryCallback callback);
    void registerOnResultTuplesCallback(ResultTuplesCallback callback);
    void registerOnSystestLogicalSourceCallback(SystestLogicalSourceCallback callback);
    void registerOnCSVSourceCallback(CSVSourceCallback callback);
    void registerOnSystestSinkCallback(SystestSinkCallback callback);
    void registerOnErrorExpectationCallback(ErrorExpectationCallback callback);

    void parse();

private:
    /// Substitution rules ///
    std::vector<SubstitutionRule> substitutionRules;
    void applySubstitutionRules(std::string& line);

    /// Parsing utils ///
    [[nodiscard]] static std::optional<TokenType> getTokenIfValid(std::string potentialToken);
    /// Parse the next token and return its type.
    [[nodiscard]] std::optional<TokenType> getNextToken();
    /// Got the next token. Returns false if reached end of file.
    [[nodiscard]] bool moveToNextToken();
    /// Look ahead at the next token without consuming it
    [[nodiscard]] std::optional<TokenType> peekToken() const;

    [[nodiscard]] SystestLogicalSource expectSystestLogicalSource();
    [[nodiscard]] CSVSource expectCSVSource() const;
    [[nodiscard]] SystestSink expectSink() const;
    [[nodiscard]] std::vector<std::string> expectTuples(bool ignoreFirst);
    [[nodiscard]] std::string expectQuery();
    [[nodiscard]] ErrorExpectation expectError() const;

    QueryCallback onQueryCallback;
    ResultTuplesCallback onResultTuplesCallback;
    SystestLogicalSourceCallback onSystestLogicalSourceCallback;
    CSVSourceCallback onCSVSourceCallback;
    SystestSinkCallback onSystestSinkCallback;
    ErrorExpectationCallback onErrorExpectationCallback;

    bool firstToken = true;
    size_t currentLine = 0;
    std::vector<std::string> lines;
};
}
