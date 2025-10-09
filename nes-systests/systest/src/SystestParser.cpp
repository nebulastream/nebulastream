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

#include <SystestParser.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <optional>
#include <ranges>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestState.hpp>

namespace
{

bool emptyOrComment(const std::string& line)
{
    return line.empty() /// completely empty
        || line.find_first_not_of(" \t\n\r\f\v") == std::string::npos /// only whitespaces
        || line.starts_with('#'); /// slt comment
}

}

namespace NES::Systest
{

static constexpr auto CreateToken = "CREATE"s;
static constexpr auto QueryToken = "SELECT"s;
static constexpr auto ResultDelimiter = "----"s;
static constexpr auto ErrorToken = "ERROR"s;
static constexpr auto DifferentialToken = "===="s;

static const std::array stringToToken = std::to_array<std::pair<std::string_view, TokenType>>(
    {{CreateToken, TokenType::CREATE},
     {QueryToken, TokenType::QUERY},
     {ResultDelimiter, TokenType::RESULT_DELIMITER},
     {ErrorToken, TokenType::ERROR_EXPECTATION},
     {DifferentialToken, TokenType::DIFFERENTIAL}});

/// We do not load the file in a constructor, as we want to be able to handle errors
bool SystestParser::loadFile(const std::filesystem::path& filePath)
{
    std::ifstream infile(filePath);
    if (!infile.is_open() || infile.bad())
    {
        return false;
    }
    std::stringstream buffer;
    buffer << infile.rdbuf();
    return loadString(buffer.str());
}

bool SystestParser::loadString(const std::string& str)
{
    currentLine = 0;
    lines.clear();

    std::istringstream stream(str);
    std::string line;
    while (std::getline(stream, line))
    {
        /// Remove commented code
        const size_t commentPos = line.find('#');
        if (commentPos != std::string::npos)
        {
            line = line.substr(0, commentPos);
        }
        /// add lines that do not start with a comment
        if (commentPos != 0)
        {
            /// Add to parsing lines
            lines.push_back(line);
        }
    }
    return true;
}

void SystestParser::registerOnQueryCallback(QueryCallback callback)
{
    this->onQueryCallback = std::move(callback);
}

void SystestParser::registerOnResultTuplesCallback(ResultTuplesCallback callback)
{
    this->onResultTuplesCallback = std::move(callback);
}

void SystestParser::registerOnErrorExpectationCallback(ErrorExpectationCallback callback)
{
    this->onErrorExpectationCallback = std::move(callback);
}

void SystestParser::registerOnCreateCallback(CreateCallback callback)
{
    this->onCreateCallback = std::move(callback);
}

void SystestParser::registerOnDifferentialQueryBlockCallback(DifferentialQueryBlockCallback callback)
{
    this->onDifferentialQueryBlockCallback = std::move(callback);
}

/// Here we model the structure of the test file by what we `expect` to see.
void SystestParser::parse()
{
    SystestQueryIdAssigner queryIdAssigner{};
    while (auto token = getNextToken())
    {
        switch (token.value())
        {
            case TokenType::CREATE: {
                auto [query, testData] = expectCreateStatement();
                onCreateCallback(query, testData);
                break;
            }
            case TokenType::QUERY: {
                static const std::unordered_set<TokenType> DefaultQueryStopTokens{TokenType::RESULT_DELIMITER, TokenType::DIFFERENTIAL};

                auto query = expectQuery(DefaultQueryStopTokens);
                lastParsedQuery = query;
                auto queryId = queryIdAssigner.getNextQueryNumber();
                lastParsedQueryId = queryId;
                if (onQueryCallback)
                {
                    onQueryCallback(query, queryId);
                }
                break;
            }
            case TokenType::RESULT_DELIMITER: {
                const auto optionalToken = peekToken();
                if (optionalToken == TokenType::ERROR_EXPECTATION)
                {
                    ++currentLine;
                    auto expectation = expectError();
                    if (onErrorExpectationCallback)
                    {
                        onErrorExpectationCallback(expectation, queryIdAssigner.getNextQueryResultNumber());
                    }
                }
                else
                {
                    if (onResultTuplesCallback)
                    {
                        onResultTuplesCallback(expectTuples(false), queryIdAssigner.getNextQueryResultNumber());
                    }
                }
                break;
            }
            case TokenType::DIFFERENTIAL: {
                INVARIANT(lastParsedQuery.has_value() && lastParsedQueryId.has_value(), "Differential block without preceding query");

                auto [leftQuery, rightQuery] = expectDifferentialBlock();
                const auto mainQueryId = lastParsedQueryId.value();
                auto differentialQueryId = queryIdAssigner.getNextQueryResultNumber();

                lastParsedQuery = rightQuery;
                lastParsedQueryId = differentialQueryId;

                if (onDifferentialQueryBlockCallback)
                {
                    onDifferentialQueryBlockCallback(std::move(leftQuery), std::move(rightQuery), mainQueryId, differentialQueryId);
                }
                break;
            }
            case TokenType::ERROR_EXPECTATION:
                throw TestException(
                    "Should never run into the ERROR_EXPECTATION token during systest file parsing, but got line: {}", lines[currentLine]);
        }
    }
}

std::optional<TokenType> SystestParser::getTokenIfValid(const std::string& line)
{
    /// Query is a special case as it's identifying token is not space seperated
    if (Util::toLowerCase(line).starts_with(Util::toLowerCase(QueryToken)))
    {
        return TokenType::QUERY;
    }

    std::string potentialToken;
    std::istringstream stream(line);
    stream >> potentialToken;

    /// Lookup in map
    const auto* it = std::ranges::find_if(
        stringToToken, [&potentialToken](const auto& pair) { return Util::toLowerCase(pair.first) == Util::toLowerCase(potentialToken); });
    if (it != stringToToken.end())
    {
        return it->second;
    }
    return std::nullopt;
}

bool SystestParser::moveToNextToken()
{
    /// Do not move to next token if its the first
    if (firstToken)
    {
        firstToken = false;
    }
    else if (shouldRevisitCurrentLine)
    {
        shouldRevisitCurrentLine = false;
    }
    else
    {
        ++currentLine;
    }

    /// Ignore comments
    while (currentLine < lines.size() && emptyOrComment(lines[currentLine]))
    {
        ++currentLine;
    }

    /// Return false if we reached the end of the file
    return currentLine < lines.size();
}

std::optional<TokenType> SystestParser::getNextToken()
{
    if (!moveToNextToken())
    {
        return std::nullopt;
    }

    const std::string line = lines[currentLine];

    INVARIANT(!line.empty(), "a potential token should never be empty");

    if (auto token = getTokenIfValid(line); token.has_value())
    {
        return token;
    }

    throw SLTUnexpectedToken("Should never run into the INVALID token during systest file parsing, but got line: {}.", lines[currentLine]);
}

std::optional<TokenType> SystestParser::peekToken() const
{
    size_t peekLine = currentLine + 1;
    /// Skip empty lines and comments
    while (peekLine < lines.size() && emptyOrComment(lines[peekLine]))
    {
        ++peekLine;
    }
    if (peekLine >= lines.size())
    {
        return std::nullopt;
    }

    const std::string line = lines[peekLine];

    INVARIANT(!line.empty(), "a potential token should never be empty");
    return getTokenIfValid(line);
}

std::vector<std::string> SystestParser::expectTuples(const bool ignoreFirst)
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist: {}", currentLine);
    std::vector<std::string> tuples;
    /// skip the result line `----`
    if (currentLine < lines.size() && (Util::toLowerCase(lines[currentLine]) == Util::toLowerCase(ResultDelimiter) || ignoreFirst))
    {
        currentLine++;
    }
    /// read the tuples until we encounter an empty line or the next token
    while (currentLine < lines.size())
    {
        if (lines[currentLine].empty())
        {
            break;
        }

        std::string potentialToken;
        std::istringstream stream(lines[currentLine]);
        if (stream >> potentialToken)
        {
            if (auto tokenType = getTokenIfValid(potentialToken); tokenType.has_value())
            {
                break;
            }
        }

        tuples.push_back(lines[currentLine]);
        currentLine++;
    }
    return tuples;
}

std::pair<std::string, std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>> SystestParser::expectCreateStatement()
{
    std::string createQuery;
    std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>> testData = std::nullopt;

    while (currentLine < lines.size())
    {
        const std::string line = lines[currentLine];
        if (emptyOrComment(line))
        {
            currentLine++;
            continue;
        }
        createQuery += line;
        if (createQuery.ends_with(';'))
        {
            break;
        }
        createQuery += '\n';
        currentLine++;
    }

    if (currentLine + 1 < lines.size() && lines[currentLine + 1].starts_with("ATTACH INLINE"))
    {
        testData = std::make_pair(TestDataIngestionType::INLINE, std::vector<std::string>{});
        currentLine += 2;
        while (currentLine < lines.size() && !lines[currentLine].empty())
        {
            testData.value().second.push_back(lines[currentLine]);
            currentLine++;
        }
        currentLine--;
    }
    else if (currentLine + 1 < lines.size() && lines[currentLine + 1].starts_with("ATTACH FILE"))
    {
        testData = std::make_pair(TestDataIngestionType::FILE, std::vector<std::string>{});
        currentLine += 1;
        testData->second.push_back(lines[currentLine].substr(std::strlen("ATTACH FILE") + 1));
    }


    return std::make_pair(createQuery, testData);
}

std::string SystestParser::expectQuery()
{
    return expectQuery({TokenType::RESULT_DELIMITER});
}

std::string SystestParser::expectQuery(const std::unordered_set<TokenType>& stopTokens)
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    std::string queryString;
    while (currentLine < lines.size())
    {
        const auto& line = lines[currentLine];
        if (emptyOrComment(line))
        {
            if (!queryString.empty())
            {
                const auto trimmedQuerySoFar = Util::trimWhiteSpaces(std::string_view(queryString));
                if (!trimmedQuerySoFar.empty() && trimmedQuerySoFar.back() == ';')
                {
                    break;
                }
            }
            ++currentLine;
            continue;
        }

        /// Check if we've reached a stop token
        std::string potentialToken;
        std::istringstream stream(line);
        if (stream >> potentialToken)
        {
            if (auto tokenType = getTokenIfValid(potentialToken); tokenType.has_value())
            {
                if (stopTokens.contains(tokenType.value()))
                {
                    break;
                }
            }
            else
            {
                const auto trimmedLineView = Util::trimWhiteSpaces(std::string_view(line));
                if (!trimmedLineView.empty() && Util::toLowerCase(trimmedLineView) == "differential")
                {
                    throw SLTUnexpectedToken(
                        "Expected differential delimiter '{}' but encountered legacy keyword '{}'", DifferentialToken, line);
                }
            }
        }

        if (!queryString.empty())
        {
            queryString += "\n";
        }
        queryString += line;
        ++currentLine;
    }

    if (queryString.empty())
    {
        throw SLTUnexpectedToken("Expected query but got empty query string");
    }

    shouldRevisitCurrentLine = currentLine < lines.size();
    return queryString;
}

std::pair<std::string, std::string> SystestParser::expectDifferentialBlock()
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");
    INVARIANT(lastParsedQuery.has_value(), "Differential block must follow a query definition");

    std::string potentialToken;
    std::istringstream stream(lines[currentLine]);
    if (!(stream >> potentialToken))
    {
        throw SLTUnexpectedToken("Expected differential delimiter at current line");
    }

    auto tokenOpt = getTokenIfValid(potentialToken);
    if (!tokenOpt.has_value() || tokenOpt.value() != TokenType::DIFFERENTIAL)
    {
        throw SLTUnexpectedToken("Expected differential delimiter at current line");
    }

    /// Skip the differential delimiter line
    ++currentLine;
    shouldRevisitCurrentLine = false;

    static const std::unordered_set<TokenType> differentialStopTokens{
        TokenType::RESULT_DELIMITER, TokenType::DIFFERENTIAL, TokenType::ERROR_EXPECTATION, TokenType::CREATE};

    /// Parse the differential query until the next recognized section
    std::string rightQuery = expectQuery(differentialStopTokens);
    const std::string leftQuery = lastParsedQuery.value();

    return {leftQuery, std::move(rightQuery)};
}

SystestParser::ErrorExpectation SystestParser::expectError() const
{
    /// Expects the form:
    /// ERROR <CODE> "optional error message to check"
    /// ERROR <ERRORTYPE STR> "optional error message to check"
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    ErrorExpectation expectation;
    const auto& line = lines[currentLine];
    std::istringstream stream(line);

    /// Skip the ERROR token
    std::string token;
    stream >> token;
    INVARIANT(Util::toLowerCase(token) == Util::toLowerCase(ErrorToken), "Expected ERROR token");

    /// Read the error code
    std::string errorStr;
    if (!(stream >> errorStr))
    {
        throw SLTUnexpectedToken("failed to read error code in: {}", line);
    }

    const std::regex numberRegex("^\\d+$");
    if (std::regex_match(errorStr, numberRegex))
    {
        /// String is a valid integer
        auto code = std::stoul(errorStr);
        if (!errorCodeExists(code))
        {
            throw SLTUnexpectedToken("invalid error code: {} is not defined in ErrorDefinitions.inc", errorStr);
        }
        expectation.code = static_cast<ErrorCode>(code);
    }
    else if (auto codeOpt = errorTypeExists(errorStr))
    {
        expectation.code = codeOpt.value();
    }
    else
    {
        throw SLTUnexpectedToken("invalid error type: {} is not defined in ErrorDefinitions.inc", errorStr);
    }

    /// Read optional error message
    std::string message;
    if (std::getline(stream, message))
    {
        /// Trim leading whitespace
        message.erase(0, message.find_first_not_of(" \t"));
        if (!message.empty())
        {
            /// Validate quotes are properly paired
            if (message.front() == '"')
            {
                if (message.back() != '"')
                {
                    throw SLTUnexpectedToken("unmatched quote in error message: {}", message);
                }
                message = message.substr(1, message.length() - 2);
            }
            else if (message.back() == '"')
            {
                throw SLTUnexpectedToken("unmatched quote in error message: {}", message);
            }
            expectation.message = message;
        }
    }

    return expectation;
}
}
