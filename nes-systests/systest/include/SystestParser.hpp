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
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestState.hpp>


namespace NES::Systest
{
using namespace std::literals;

/// Tokens ///
enum class TokenType : uint8_t
{
    INVALID,
    LOGICAL_SOURCE,
    ATTACH_SOURCE,
    SINK,
    QUERY,
    RESULT_DELIMITER,
    ERROR_EXPECTATION,
};

/// Assures that the number of parsed queries matches the number of parsed results
class SystestQueryIdAssigner
{
    static constexpr SystestQueryId::Underlying INITIAL_QUERY_NUMBER = SystestQueryId::INITIAL;

public:
    explicit SystestQueryIdAssigner() = default;

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

struct SystestField
{
    DataType type;
    std::string name;
    friend std::ostream& operator<<(std::ostream& os, const SystestField& field)
    {
        os << fmt::format("{} {}", magic_enum::enum_name(field.type.type), field.name);
        return os;
    }
    bool operator==(const SystestField& other) const = default;
    bool operator!=(const SystestField& other) const = default;
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
    struct SystestLogicalSource
    {
        std::string name;
        SystestSchema fields;
        bool operator==(const SystestLogicalSource& other) const = default;
    };

    struct SystestSink
    {
        std::string name;
        std::string type;
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
    using SystestAttachSourceCallback = std::function<void(SystestAttachSource attachSource)>;
    using SystestSinkCallback = std::function<void(SystestSink&&)>;
    using ErrorExpectationCallback = std::function<void(const ErrorExpectation&, SystestQueryId correspondingQueryId)>;

    /// Register callbacks to be called when the respective section is parsed
    void registerOnQueryCallback(QueryCallback callback);
    void registerOnResultTuplesCallback(ResultTuplesCallback callback);
    void registerOnSystestLogicalSourceCallback(SystestLogicalSourceCallback callback);
    void registerOnSystestAttachSourceCallback(SystestAttachSourceCallback callback);
    void registerOnSystestSinkCallback(SystestSinkCallback callback);
    void registerOnErrorExpectationCallback(ErrorExpectationCallback callback);

    void parse();
    void parseResultLines();

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

    [[nodiscard]] std::pair<SystestLogicalSource, std::optional<SystestAttachSource>> expectSystestLogicalSource();
    [[nodiscard]] SystestAttachSource expectAttachSource();
    [[nodiscard]] SystestSink expectSink() const;
    [[nodiscard]] std::vector<std::string> expectTuples(bool ignoreFirst);
    [[nodiscard]] std::filesystem::path expectFilePath();
    [[nodiscard]] std::string expectQuery();
    [[nodiscard]] ErrorExpectation expectError() const;

    QueryCallback onQueryCallback;
    ResultTuplesCallback onResultTuplesCallback;
    SystestLogicalSourceCallback onSystestLogicalSourceCallback;
    SystestAttachSourceCallback onAttachSourceCallback;
    SystestSinkCallback onSystestSinkCallback;
    ErrorExpectationCallback onErrorExpectationCallback;

    bool firstToken = true;
    size_t currentLine = 0;
    std::vector<std::string> lines;
    std::unordered_set<std::string> seenLogicalSourceNames;
};
}

FMT_OSTREAM(NES::Systest::SystestField);
