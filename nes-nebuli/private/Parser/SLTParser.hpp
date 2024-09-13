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

#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Util/Logger/Logger.hpp>

namespace NES
{
using namespace std::literals;

/// This is a parser for a dialect of the sqllogictest format. We follow a pull-based parser design as proposed in:
/// https://www.think-cell.com/assets/en/career/talks/pdf/think-cell_talk_json.pdf
///
/// NOTE: register substitution rules before calling `loadFile`
/// NOTE: register callbacks before calling `parse`
class SLTParser
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

    [[nodiscard]] bool loadFile(const std::string& filePath);

    using QueryCallback = std::function<void(const std::string& /* Query String */)>;
    using QueryResultTuplesCallback = std::function<void(std::vector<std::string>&& /* Tpls */)>;
    using SourceInFileTuplesCallback
        = std::function<void(std::vector<std::string>&& /* Source Def */, std::vector<std::string>&& /* Tpls */)>;
    using SourceCallback = std::function<void(std::vector<std::string>&& /* Source Def */)>;

    /// Register callbacks to be called when the respective section is parsed
    void registerOnQueryCallback(QueryCallback callback);
    void registerOnQueryResultTuplesCallback(QueryResultTuplesCallback callback);
    void registerOnSourceInFileTuplesCallback(SourceInFileTuplesCallback callback);
    void registerOnSourceCallback(SourceCallback callback);

    [[nodiscard]] bool parse();

private:
    std::vector<SubstitutionRule> substitutionRules;
    void applySubstitutionRules(std::string& line);

    enum class TokenType : uint8_t
    {
        INVALID,
        SOURCE_CSV,
        SOURCE,
        QUERY,
        RESULT_DELIMITER,
        END,
    };

    inline bool isSource(TokenType type) const noexcept { return type == TokenType::SOURCE; }
    inline bool isCSVSource(TokenType type) const noexcept { return type == TokenType::SOURCE_CSV; }
    inline bool isQuery(TokenType type) const noexcept { return type == TokenType::QUERY; }
    inline bool isInvalid(TokenType type) const noexcept { return type == TokenType::INVALID; }
    inline bool isEnd(TokenType type) const noexcept { return type == TokenType::END; }
    inline bool isResultDelimiter(TokenType type) const noexcept { return type == TokenType::RESULT_DELIMITER; }

    static constexpr std::array<std::pair<std::string_view, TokenType>, 4> stringToToken
        = {{{"SourceCSV"sv, TokenType::SOURCE_CSV},
            {"Source"sv, TokenType::SOURCE},
            {"Query::from"sv, TokenType::QUERY},
            {"----"sv, TokenType::RESULT_DELIMITER}}};

    [[nodiscard]] TokenType nextToken();

    [[nodiscard]] std::vector<std::string> expectSource();
    [[nodiscard]] std::vector<std::string> expectTuples(bool ignoreFirst = false);
    [[nodiscard]] std::string expectQuery();

    [[nodiscard]] static bool emptyOrComment(const std::string& line);

    QueryCallback onQueryCallback;
    QueryResultTuplesCallback onQueryResultTuplesCallback;
    SourceInFileTuplesCallback onSourceInFileTuplesCallback;
    SourceCallback onSourceCallback;

    size_t currentLine = 0;
    std::vector<std::string> lines;
};
}
