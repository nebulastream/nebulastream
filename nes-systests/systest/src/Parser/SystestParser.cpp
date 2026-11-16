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

#include <Parser/SystestParser.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

enum class TokenType : uint8_t
{
    SELECT,
    EXPLAIN,
    CREATE,
    RESULT,
    ERROR,
    CONFIGURATION,
    GLOBAL_CONFIGURATION,
    DIFFERENTIAL,
};

constexpr std::string_view CreateToken = "CREATE";
constexpr std::string_view SelectToken = "SELECT";
constexpr std::string_view ExplainToken = "EXPLAIN";
constexpr std::string_view ResultToken = "----";
constexpr std::string_view ErrorToken = "ERROR";
constexpr std::string_view DifferentialToken = "====";
constexpr std::string_view ConfigurationToken = "CONFIGURATION";
constexpr std::string_view GlobalConfigurationToken = "GLOBALCONFIGURATION";
constexpr std::string_view AttachInlineToken = "ATTACH INLINE";
constexpr std::string_view AttachFileToken = "ATTACH FILE";
constexpr std::string_view PlanEndToken = "==END==";

constexpr std::string_view Whitespace = " \t\n\v\f\r";

constexpr auto TokenToType = std::to_array<std::pair<std::string_view, TokenType>>(
    {{CreateToken, TokenType::CREATE},
     {SelectToken, TokenType::SELECT},
     {ExplainToken, TokenType::EXPLAIN},
     {ResultToken, TokenType::RESULT},
     {ErrorToken, TokenType::ERROR},
     {ConfigurationToken, TokenType::CONFIGURATION},
     {GlobalConfigurationToken, TokenType::GLOBAL_CONFIGURATION},
     {DifferentialToken, TokenType::DIFFERENTIAL}});

constexpr std::array QueryStopTokens{TokenType::RESULT, TokenType::DIFFERENTIAL};
constexpr std::array DifferentialStopTokens{
    TokenType::RESULT,
    TokenType::DIFFERENTIAL,
    TokenType::ERROR,
    TokenType::CREATE,
    TokenType::CONFIGURATION,
    TokenType::GLOBAL_CONFIGURATION};

struct SplitWord
{
    std::string_view word;
    std::string_view rest;
};

SplitWord splitFirstWord(const std::string_view text)
{
    const auto start = std::min(text.find_first_not_of(Whitespace), text.size());
    const auto end = std::min(text.find_first_of(Whitespace, start), text.size());
    return {.word = text.substr(start, end - start), .rest = text.substr(end)};
}

std::string_view trimLeading(const std::string_view text, const std::string_view characters)
{
    return text.substr(std::min(text.find_first_not_of(characters), text.size()));
}

bool isBlankOrComment(const std::string_view line)
{
    return line.find_first_not_of(Whitespace) == std::string_view::npos || line.starts_with('#');
}

bool isIdentifierCharacter(const char character)
{
    return std::isalnum(static_cast<unsigned char>(character)) != 0 || character == '_';
}

void applySubstitutions(std::string& line, const std::span<const Substitution> substitutions)
{
    for (const auto& [keyword, replacement] : substitutions)
    {
        for (auto pos = line.find(keyword); pos != std::string::npos; pos = line.find(keyword, pos))
        {
            /// A keyword is replaced only as a whole word, so "we" inside "producedPower" stays.
            const auto before = std::string_view{line}.substr(0, pos);
            const auto after = std::string_view{line}.substr(pos + keyword.length());
            const auto leftBoundary = before.empty() || !isIdentifierCharacter(before.back());
            const auto rightBoundary = after.empty() || !isIdentifierCharacter(after.front());

            if (leftBoundary && rightBoundary)
            {
                line.replace(pos, keyword.length(), replacement);
                pos += replacement.length();
            }
            else
            {
                pos += keyword.length();
            }
        }
    }
}

std::vector<std::string> readLines(const std::string_view content, const std::span<const Substitution> substitutions)
{
    std::vector<std::string> lines;
    std::istringstream stream{std::string{content}};
    std::string line;
    while (std::getline(stream, line))
    {
        /// A line starting with `#` is dropped rather than kept as a blank line, so it does not end a block of rows.
        if (const auto commentPos = line.find('#'); commentPos != 0)
        {
            lines.push_back(line.substr(0, commentPos));
            applySubstitutions(lines.back(), substitutions);
        }
    }
    return lines;
}

std::optional<TokenType> findToken(const std::string_view line)
{
    /// SELECT matches as a prefix of the whole line, so a query needs no space after the keyword.
    if (toUpperCase(line.substr(0, SelectToken.size())) == SelectToken)
    {
        return TokenType::SELECT;
    }

    const auto firstWord = toUpperCase(splitFirstWord(line).word);
    for (const auto& [token, type] : TokenToType)
    {
        if (token == firstWord)
        {
            return type;
        }
    }
    return std::nullopt;
}

class LineCursor
{
public:
    explicit LineCursor(const std::span<const std::string> lines) : lines{lines} { }

    [[nodiscard]] std::optional<std::string_view> peek() const
    {
        if (lines.empty())
        {
            return std::nullopt;
        }
        return lines.front();
    }

    std::optional<std::string_view> next()
    {
        const auto line = peek();
        skip();
        return line;
    }

    void skip()
    {
        if (!lines.empty())
        {
            lines = lines.subspan(1);
        }
    }

    void skipBlank() { lines = {std::ranges::find_if_not(lines, isBlankOrComment), lines.end()}; }

    [[nodiscard]] std::vector<std::string> takeWhile(const std::predicate<std::string_view> auto& predicate)
    {
        const auto end = std::ranges::find_if_not(lines, predicate);
        std::vector<std::string> taken{lines.begin(), end};
        lines = {end, lines.end()};
        return taken;
    }

private:
    std::span<const std::string> lines;
};

struct TokenLine
{
    TokenType type;
    std::string_view line;
};

std::optional<TokenLine> getNextToken(LineCursor& cursor)
{
    cursor.skipBlank();
    const auto line = cursor.peek();
    if (!line.has_value())
    {
        return std::nullopt;
    }
    if (const auto type = findToken(*line); type.has_value())
    {
        return TokenLine{.type = *type, .line = *line};
    }
    throw SLTUnexpectedToken("Should never run into the INVALID token during systest file parsing, but got line: {}.", *line);
}

std::optional<TokenType> peekTokenAfter(LineCursor cursor)
{
    cursor.skip();
    cursor.skipBlank();
    return cursor.peek().and_then(findToken);
}

std::vector<ConfigurationOverride> parseConfigurationLine(const std::string_view line, const std::string_view kindLabel)
{
    const auto [keyWithColon, afterKey] = splitFirstWord(splitFirstWord(line).rest);
    if (!keyWithColon.ends_with(':'))
    {
        throw SLTUnexpectedToken("Expected colon at end of key: '{}'", keyWithColon);
    }
    const auto key = keyWithColon.substr(0, keyWithColon.size() - 1);
    if (key.empty())
    {
        throw SLTUnexpectedToken("Expected {} key before colon, but got empty key", kindLabel);
    }

    const auto valueList = trimLeading(afterKey, Whitespace);
    if (valueList.empty())
    {
        throw SLTUnexpectedToken("Expected {} value after key '{}', but got empty value", kindLabel, key);
    }

    const auto invalidFormat = [&](const std::string_view details)
    { throw SLTUnexpectedToken("Invalid {} format for key '{}': '{}'. {}", kindLabel, key, valueList, details); };

    std::vector<std::string> values;
    if (valueList.starts_with('[') && valueList.ends_with(']'))
    {
        const auto insideBrackets = valueList.substr(1, valueList.size() - 2);
        if (insideBrackets.empty())
        {
            invalidFormat("Expected at least one value inside the brackets");
        }
        values = splitWithStringDelimiter<std::string>(insideBrackets, ",");
    }
    else
    {
        if (valueList.find_first_of("[]") != std::string_view::npos)
        {
            invalidFormat("Use either single value or properly formatted list in square brackets");
        }
        if (valueList.contains(','))
        {
            invalidFormat("Multiple values must be enclosed in square brackets");
        }
        values = {std::string{valueList}};
    }

    std::vector<ConfigurationOverride> result;
    for (const auto& rawValue : values)
    {
        const auto value = std::string{trimWhiteSpaces(rawValue)};
        if (value.empty())
        {
            throw SLTUnexpectedToken("Empty {} value found for key '{}'", kindLabel, key);
        }
        result.push_back(ConfigurationOverride{{std::string{key}, value}});
    }
    return result;
}

/// Reads `ERROR <code> [message]`, where the code is a number or an error type and the message may be quoted.
ExpectedError parseErrorLine(const std::string_view line)
{
    const auto [keyword, afterKeyword] = splitFirstWord(line);
    if (toUpperCase(keyword) != ErrorToken)
    {
        throw SLTUnexpectedToken("Expected {} on the line directly after {}, but got: {}", ErrorToken, ResultToken, line);
    }

    const auto [codeText, afterCode] = splitFirstWord(afterKeyword);
    if (codeText.empty())
    {
        throw SLTUnexpectedToken("failed to read error code in: {}", line);
    }
    const auto isNumber = std::ranges::all_of(codeText, [](const unsigned char character) { return std::isdigit(character) != 0; });
    const auto code = isNumber ? from_chars<uint64_t>(codeText).and_then(errorCodeExists) : errorTypeExists(codeText);
    if (!code.has_value())
    {
        throw SLTUnexpectedToken("invalid error {}: {} is not defined in ErrorDefinitions.inc", isNumber ? "code" : "type", codeText);
    }

    const auto message = trimLeading(afterCode, " \t");
    if (message.empty())
    {
        return ExpectedError{.code = code.value(), .message = std::nullopt};
    }
    const auto quoted = message.starts_with('"');
    if (quoted != message.ends_with('"'))
    {
        throw SLTUnexpectedToken("unmatched quote in error message: {}", message);
    }
    if (!quoted)
    {
        return ExpectedError{.code = code.value(), .message = std::string{message}};
    }
    const auto unquoted = message.size() < 2 ? std::string_view{} : message.substr(1, message.size() - 2);
    return ExpectedError{.code = code.value(), .message = std::string{unquoted}};
}

std::vector<std::string> expectRows(LineCursor& cursor)
{
    if (cursor.peek() == ResultToken)
    {
        cursor.skip();
    }
    auto rows
        = cursor.takeWhile([](const std::string_view line) { return !line.empty() && !findToken(splitFirstWord(line).word).has_value(); });
    /// Skipping the line that ends the rows drops a statement written directly below them without a blank line.
    cursor.skip();
    return rows;
}

std::vector<std::string> expectPlan(LineCursor& cursor)
{
    if (cursor.peek() == ResultToken)
    {
        cursor.skip();
    }
    auto planLines = cursor.takeWhile([](const std::string_view line) { return !line.starts_with(PlanEndToken); });
    cursor.skip();
    return planLines;
}

Expectation expectResult(LineCursor& cursor, const bool isPlan)
{
    /// The lookahead skips blank lines, but the error has to be on the line directly after the delimiter.
    if (peekTokenAfter(cursor) == TokenType::ERROR)
    {
        cursor.skip();
        return parseErrorLine(cursor.next().value_or(""));
    }
    if (isPlan)
    {
        return ExpectedPlan{.lines = expectPlan(cursor)};
    }
    return ExpectedRows{.rows = expectRows(cursor)};
}

std::optional<AttachedData> expectAttach(LineCursor& cursor)
{
    const auto attachLine = cursor.peek().value_or("");
    if (attachLine.starts_with(AttachInlineToken))
    {
        cursor.skip();
        return InlineRows{.rows = cursor.takeWhile([](const std::string_view line) { return !line.empty(); })};
    }
    if (attachLine.starts_with(AttachFileToken))
    {
        const auto afterToken = attachLine.substr(AttachFileToken.size());
        if (afterToken.empty())
        {
            throw SLTUnexpectedToken("Expected a path after '{}'", AttachFileToken);
        }
        cursor.skip();
        return AttachedFile{.path = std::filesystem::path{afterToken.substr(1)}};
    }
    return std::nullopt;
}

CreateStatement expectCreate(LineCursor& cursor)
{
    std::string sql;
    while (const auto line = cursor.next())
    {
        if (isBlankOrComment(*line))
        {
            continue;
        }
        sql += *line;
        if (sql.ends_with(';'))
        {
            break;
        }
        sql += '\n';
    }
    cursor.skipBlank();
    return CreateStatement{.sql = std::move(sql), .attach = expectAttach(cursor)};
}

std::string expectQuery(LineCursor& cursor, const std::span<const TokenType> stopTokens)
{
    std::string sql;
    while (const auto line = cursor.peek())
    {
        if (isBlankOrComment(*line))
        {
            if (trimWhiteSpaces(sql).ends_with(';'))
            {
                break;
            }
            cursor.skip();
            continue;
        }

        if (const auto type = findToken(*line); type.has_value() && std::ranges::contains(stopTokens, type.value()))
        {
            if (const auto trimmedSql = trimWhiteSpaces(sql); !trimmedSql.ends_with(';'))
            {
                throw InvalidQuerySyntax("Queries must end with a semicolon: \"{}\"", trimmedSql);
            }
            break;
        }
        if (toLowerCase(trimWhiteSpaces(*line)) == "differential")
        {
            throw SLTUnexpectedToken("Expected differential delimiter '{}' but encountered legacy keyword '{}'", DifferentialToken, *line);
        }

        if (!sql.empty())
        {
            sql += '\n';
        }
        sql += *line;
        cursor.skip();
    }

    if (sql.empty())
    {
        throw SLTUnexpectedToken("Expected query but got empty query string");
    }
    return sql;
}

/// The configuration lines in scope for the next query.
/// A global line holds until the file ends, and a later global line replaces the keys of an earlier one.
/// A local line holds for one query, and any duplicate key with other local lines in scope is an error.
/// A line may list several values, and the query runs once per pairing of values across the lines in scope.
class CurrentOverrides
{
public:
    void addGlobal(const std::vector<ConfigurationOverride>& overrides) { global = combine(overrides, global, OnDuplicateKey::Replace); }

    void addLocal(const std::vector<ConfigurationOverride>& overrides) { local = combine(overrides, local, OnDuplicateKey::Reject); }

    /// The result always holds at least one entry, so a query with no overrides runs exactly once.
    [[nodiscard]] std::vector<ConfigurationOverride> takeForNextQuery()
    {
        return combine(std::exchange(local, {}), global, OnDuplicateKey::Reject);
    }

    void discardLocal() { local = {}; }

private:
    std::vector<ConfigurationOverride> global;
    std::vector<ConfigurationOverride> local;
};

/// An EXPLAIN expects the plan it prints, so this rejects an expected error.
ExpectedPlan getExpectedPlan(const Expectation& expected)
{
    if (const auto* plan = std::get_if<ExpectedPlan>(&expected))
    {
        return *plan;
    }
    throw SLTUnexpectedToken("an EXPLAIN expects the plan it prints, not an error");
}

struct PendingQuery
{
    std::string sql;
    SystestQueryId id;
    std::vector<ConfigurationOverride> overrides;
    bool isExplain = false;
};

class StatementAssembler
{
public:
    void addCreate(CreateStatement create) { statements.emplace_back(std::move(create)); }

    void addGlobalOverrides(const std::vector<ConfigurationOverride>& line) { overrides.addGlobal(line); }

    void addLocalOverrides(const std::vector<ConfigurationOverride>& line) { overrides.addLocal(line); }

    /// Reads the overrides in scope now, rather than when the result completes the query,
    /// because a configuration line that follows the query belongs to the next one.
    void beginSelect(std::string sql)
    {
        const auto id = assignQueryId();
        pendingQuery = PendingQuery{.sql = std::move(sql), .id = id, .overrides = overrides.takeForNextQuery()};
    }

    /// The runner does not execute an EXPLAIN, and the optimizer configuration of the run determines its plan.
    /// The local lines in scope therefore have no effect, and this discards them instead of keeping them for the next query.
    void beginExplain(std::string sql)
    {
        const auto id = assignQueryId();
        overrides.discardLocal();
        pendingQuery = PendingQuery{.sql = std::move(sql), .id = id, .overrides = {}, .isExplain = true};
    }

    [[nodiscard]] bool expectsPlan() const { return pendingQuery.has_value() && pendingQuery->isExplain; }

    void completeQuery(const Expectation& expected)
    {
        auto [sql, id, queryOverrides, isExplain] = takePendingQuery();
        if (isExplain)
        {
            statements.emplace_back(ExplainStatement{.sql = std::move(sql), .id = id, .expected = getExpectedPlan(expected)});
            return;
        }
        /// One statement per pairing of overrides, all with the same number, because they are one query of the file run more than once.
        for (auto& alternative : queryOverrides)
        {
            statements.emplace_back(SelectStatement{.sql = sql, .id = id, .expected = expected, .overrides = std::move(alternative)});
        }
    }

    void completeDifferential(const std::string& secondSql)
    {
        auto [firstSql, id, blockOverrides, isExplain] = takePendingQuery();
        if (isExplain)
        {
            throw SLTUnexpectedToken("an EXPLAIN cannot be the first half of a differential block");
        }
        for (auto& alternative : blockOverrides)
        {
            statements.emplace_back(
                DifferentialStatement{
                    .firstSql = firstSql, .firstId = id, .secondSql = secondSql, .secondId = id, .overrides = std::move(alternative)});
        }
    }

    [[nodiscard]] std::vector<TestStatement> take() &&
    {
        if (pendingQuery.has_value())
        {
            throw SLTUnexpectedToken("the file ends with a query that has no result or expected error: {}", pendingQuery->sql);
        }
        return std::move(statements);
    }

private:
    [[nodiscard]] SystestQueryId assignQueryId()
    {
        if (pendingQuery.has_value())
        {
            throw SLTUnexpectedToken("a query follows the query {} before its result or expected error", pendingQuery->sql);
        }
        return SystestQueryId{nextQueryId++};
    }

    [[nodiscard]] PendingQuery takePendingQuery()
    {
        auto pending = std::exchange(pendingQuery, std::nullopt);
        if (!pending.has_value())
        {
            throw SLTUnexpectedToken("a result, an expected error, or a differential block must follow a query");
        }
        return std::move(*pending);
    }

    CurrentOverrides overrides;
    std::optional<PendingQuery> pendingQuery;
    SystestQueryId::Underlying nextQueryId = SystestQueryId::INITIAL;
    std::vector<TestStatement> statements;
};

}

std::vector<TestStatement> parseTestFile(const std::string_view content, const std::span<const Substitution> substitutions)
{
    const auto lines = readLines(content, substitutions);
    LineCursor cursor{lines};
    StatementAssembler assembler;
    while (const auto next = getNextToken(cursor))
    {
        const auto& [type, line] = *next;
        switch (type)
        {
            case TokenType::CREATE:
                assembler.addCreate(expectCreate(cursor));
                break;
            case TokenType::SELECT:
                assembler.beginSelect(expectQuery(cursor, QueryStopTokens));
                break;
            case TokenType::EXPLAIN:
                assembler.beginExplain(expectQuery(cursor, QueryStopTokens));
                break;
            case TokenType::RESULT:
                assembler.completeQuery(expectResult(cursor, assembler.expectsPlan()));
                break;
            case TokenType::CONFIGURATION:
                assembler.addLocalOverrides(parseConfigurationLine(line, "configuration"));
                cursor.skip();
                break;
            case TokenType::GLOBAL_CONFIGURATION:
                assembler.addGlobalOverrides(parseConfigurationLine(line, "global configuration"));
                cursor.skip();
                break;
            case TokenType::DIFFERENTIAL:
                cursor.skip();
                assembler.completeDifferential(expectQuery(cursor, DifferentialStopTokens));
                break;
            case TokenType::ERROR:
                throw TestException("Should never run into the ERROR token during systest file parsing, but got line: {}", line);
        }
    }
    return std::move(assembler).take();
}

}
