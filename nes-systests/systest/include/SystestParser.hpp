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
#include <string_view>
#include <unordered_set>
#include <vector>

#include <SystestSources/SourceTypes.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
using namespace std::literals;

/// Tokens ///
enum class TokenType : uint8_t
{
    INVALID,
    SLT_SOURCE,
    ATTACH_SOURCE,
    SINK,
    QUERY,
    RESULT_DELIMITER,
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

    /// Type definitions ///
    struct SystestLogicalSource
    {
        std::string name;
        SystestSchema fields;
        bool operator==(const SystestLogicalSource& other) const = default;
    };

    struct SystestSink
    {
        std::string name;
        SystestSchema fields;
        bool operator==(const SystestSink& other) const = default;
    };

    using ResultTuples = std::vector<std::string>;

    using QueryCallback = std::function<void(std::string, size_t)>;
    using ResultTuplesCallback = std::function<void(ResultTuples&&)>;
    using SystestLogicalSourceCallback = std::function<void(SystestLogicalSource&&)>;
    using AttachSourceCallback = std::function<void(SystestAttachSource attachSource)>;
    using SystestSinkCallback = std::function<void(SystestSink&&)>;

    /// Register callbacks to be called when the respective section is parsed
    void registerOnQueryCallback(QueryCallback callback);
    void registerOnSystestLogicalSourceCallback(SystestLogicalSourceCallback callback);
    void registerOnAttachSourceCallback(AttachSourceCallback callback);
    void registerOnSystestSystestSinkCallback(SystestSinkCallback callback);


    void parse(SystestStarterGlobals& systestStarterGlobals, std::string_view testFileName);
    void parseResultLines();

private:
    /// Substitution rules ///
    std::vector<SubstitutionRule> substitutionRules;
    void applySubstitutionRules(std::string& line);

    /// Parsing utils ///
    [[nodiscard]] static std::optional<TokenType> getTokenIfValid(std::string potentialToken);
    /// Parse the next token and return its type.
    [[nodiscard]] std::optional<TokenType> nextToken();
    /// Got the next token. Returns false if reached end of file.
    [[nodiscard]] bool moveToNextToken();

    [[nodiscard]] SystestLogicalSource expectSystestLogicalSource();
    [[nodiscard]] SystestAttachSource expectAttachSource();
    [[nodiscard]] SystestSink expectSink() const;
    [[nodiscard]] ResultTuples expectTuples(bool ignoreFirst = false);
    [[nodiscard]] std::filesystem::path expectFilePath();
    [[nodiscard]] std::string expectQuery();

    QueryCallback onQueryCallback;
    SystestLogicalSourceCallback onSystestLogicalSourceCallback;
    AttachSourceCallback onAttachSourceCallback;
    SystestSinkCallback onSystestSinkCallback;

    bool firstToken = true;
    size_t currentLine = 0;
    std::vector<std::string> lines;
    std::unordered_set<std::string> seenLogicalSourceNames;
};
}
