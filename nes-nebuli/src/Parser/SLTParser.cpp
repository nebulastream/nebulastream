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

#include <algorithm>
#include <array>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Parser/SLTParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::SLTParser
{
static constexpr std::string CSVSourceToken = "SourceCSV";
static constexpr std::string SLTSourceToken = "Source";
static constexpr std::string QueryToken = "Query::from";
static constexpr std::string ResultDelimiter = "----";

static constexpr std::array<std::pair<std::string_view, TokenType>, 4> stringToToken
    = {{{CSVSourceToken, TokenType::CSV_SOURCE},
        {SLTSourceToken, TokenType::SLT_SOURCE},
        {QueryToken, TokenType::QUERY},
        {ResultDelimiter, TokenType::RESULT_DELIMITER}}};

bool emptyOrComment(const std::string& line)
{
    return line.empty() /// completely empty
        || line.find_first_not_of(" \t\n\r\f\v") == std::string::npos /// only whitespaces
        || line.starts_with('#'); /// slt comment
}

void SLTParser::registerSubstitutionRule(const SubstitutionRule& rule)
{
    auto found = std::find_if(
        substitutionRules.begin(), substitutionRules.end(), [&rule](const SubstitutionRule& r) { return r.keyword == rule.keyword; });
    PRECONDITION(
        found == substitutionRules.end(),
        "substitution rule keywords must be unique. Tried to register for the second time: " << rule.keyword);
    substitutionRules.emplace_back(rule);
}

/// We do not load the file in a constructor, as we want to be able to handle errors
bool SLTParser::loadFile(const std::filesystem::path& filePath)
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

bool SLTParser::loadString(const std::string& str)
{
    currentLine = 0;
    lines.clear();

    std::istringstream stream(str);
    std::string line;
    while (std::getline(stream, line))
    {
        /// Remove commented code
        size_t const commentPos = line.find('#');
        if (commentPos != std::string::npos)
        {
            line = line.substr(0, commentPos);
        }
        /// add lines that do not start with a comment
        if (commentPos != 0)
        {
            /// Apply subsitutions & add to parsing lines
            applySubstitutionRules(line);
            lines.push_back(line);
        }
    }
    return true;
}

void SLTParser::registerOnQueryCallback(QueryCallback callback)
{
    this->onQueryCallback = std::move(callback);
}

void SLTParser::registerOnResultTuplesCallback(ResultTuplesCallback callback)
{
    this->onResultTuplesCallback = std::move(callback);
}

void SLTParser::registerOnSLTSourceCallback(SLTSourceCallback callback)
{
    this->onSLTSourceCallback = std::move(callback);
}

void SLTParser::registerOnCSVSourceCallback(CSVSourceCallback callback)
{
    this->onCSVSourceCallback = std::move(callback);
}

/// Here we model the structure of the test file by what we `expect` to see.
/// If we encounter something unexpected, we return false.
void SLTParser::parse()
{
    while (auto token = nextToken())
    {
        if (token.value() == TokenType::CSV_SOURCE)
        {
            auto source = expectCSVSource();
            if (onCSVSourceCallback)
            {
                onCSVSourceCallback(std::move(source));
            }
        }
        else if (token.value() == TokenType::SLT_SOURCE)
        {
            auto source = expectSLTSource();
            if (onSLTSourceCallback)
            {
                onSLTSourceCallback(std::move(source));
            }
        }
        else if (token.value() == TokenType::QUERY)
        {
            auto query = expectQuery();
            if (onQueryCallback)
            {
                onQueryCallback(std::move(query));
            }
            token = nextToken();
            if (token.has_value() && token.value() == TokenType::RESULT_DELIMITER)
            {
                auto tuples = expectTuples();
                if (onResultTuplesCallback)
                {
                    onResultTuplesCallback(std::move(tuples));
                }
            }
            else
            {
                throw SLTUnexpectedToken("expected result delimiter `{}` after query", ResultDelimiter);
            }
        }
        else if (token.value() == TokenType::RESULT_DELIMITER)
        {
            throw SLTUnexpectedToken("unexpected occurence of result delimiter {} `", ResultDelimiter);
        }
        else if (token.value() == TokenType::INVALID)
        {
            throw SLTUnexpectedToken("got invalid token in line: {}", lines[currentLine]);
        }
    }
}

void SLTParser::applySubstitutionRules(std::string& line)
{
    for (const auto& rule : substitutionRules)
    {
        size_t pos = 0;
        const std::string& keyword = rule.keyword;

        while ((pos = line.find(keyword, pos)) != std::string::npos)
        {
            /// Apply the substitution function to the part of the string found
            std::string substring = line.substr(pos, keyword.length());
            rule.ruleFunction(substring);

            /// Replace the found substring with the modified substring
            line.replace(pos, keyword.length(), substring);
            pos += substring.length();
        }
    }
}

std::optional<TokenType> SLTParser::getTokenIfValid(std::string potentialToken)
{
    /// Query is a special case as it's idenfying token is not space seperated
    if (potentialToken.compare(0, QueryToken.size(), QueryToken) == 0)
    {
        return TokenType::QUERY;
    }
    /// Lookup in map
    const auto* it = std::find_if(
        stringToToken.begin(), stringToToken.end(), [&potentialToken](const auto& pair) { return pair.first == potentialToken; });
    if (it != stringToToken.end())
    {
        return it->second;
    }
    return std::nullopt;
}

bool SLTParser::moveToNextToken()
{
    /// Do not move to next token if its the first
    if (firstToken)
    {
        firstToken = false;
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


std::optional<TokenType> SLTParser::nextToken()
{
    if (!moveToNextToken())
    {
        return std::nullopt;
    }

    std::string potentialToken;
    std::istringstream stream(lines[currentLine]);
    stream >> potentialToken;

    INVARIANT(!potentialToken.empty(), "a potential token should never be empty");

    return getTokenIfValid(potentialToken);
}

SLTParser::SLTSource SLTParser::expectSLTSource()
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    SLTSource source;
    auto& line = lines[currentLine];
    std::istringstream stream(line);

    /// Read and discard the first word as it is always Source
    std::string discard;
    if (!(stream >> discard))
    {
        throw SLTUnexpectedToken("failed to read the first word in: {}", line);
    }
    INVARIANT(discard == SLTSourceToken, "Expected first word to be `{}` for source statement", SLTSourceToken);

    /// Read the source name and check if successful
    if (!(stream >> source.name))
    {
        throw SLTUnexpectedToken("failed to read source name in: {}", line);
    }

    std::vector<std::string> arguments;
    std::string argument;
    while (stream >> argument)
    {
        arguments.push_back(argument);
    }

    if (arguments.size() % 2 != 0)
    {
        throw SLTUnexpectedToken("Incomplete fieldtype/fieldname pair for line: {}", line);
    }

    for (size_t i = 0; i < arguments.size(); i += 2)
    {
        std::string fieldtype = arguments[i];
        std::string fieldname = arguments[i + 1];
        if (auto type = magic_enum::enum_cast<BasicType>(fieldtype))
        {
            source.fields.emplace_back(type.value(), fieldname);
        }
        else
        {
            throw SLTUnexpectedToken("Unknown basic type: {}", fieldtype);
        }
    }

    /// After the source defintion line we expect result tuples
    source.tuples = expectTuples(true);

    return source;
}


SLTParser::CSVSource SLTParser::expectCSVSource() const
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");
    CSVSource source;
    auto& line = lines[currentLine];
    std::istringstream stream(line);

    /// Read and discard the first word as it is always CSVSource
    std::string discard;
    if (!(stream >> discard))
    {
        throw SLTUnexpectedToken("failed to read the first word in: {}", line);
    }
    INVARIANT(discard == CSVSourceToken, "Expected first word to be `" + CSVSourceToken + "` for csv source statement");

    /// Read the source name and check if successful
    if (!(stream >> source.name))
    {
        throw SLTUnexpectedToken("failed to read source name in: {}", line);
    }

    std::vector<std::string> arguments;
    std::string argument;
    while (stream >> argument)
    {
        arguments.push_back(argument);
    }

    source.csvFilePath = arguments.back();
    arguments.pop_back();

    if (arguments.size() % 2 != 0)
    {
        throw SLTUnexpectedToken("Incomplete fieldtype/fieldname pair for line: {}", line);
    }

    for (size_t i = 0; i < arguments.size(); i += 2)
    {
        std::string const fieldtype = arguments[i];
        std::string const fieldname = arguments[i + 1];
        if (auto type = magic_enum::enum_cast<BasicType>(fieldtype))
        {
            source.fields.emplace_back(type.value(), fieldname);
        }
        else
        {
            throw SLTUnexpectedToken("Unknown basic type: {}", fieldtype);
        }
    }
    return source;
}

SLTParser::ResultTuples SLTParser::expectTuples(bool ignoreFirst)
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    std::vector<std::string> tuples;
    /// skip the result line `----`
    if (currentLine < lines.size() && (lines[currentLine] == ResultDelimiter || ignoreFirst))
    {
        currentLine++;
    }
    /// read the tuples until we encounter an empty line
    while (currentLine < lines.size() && !lines[currentLine].empty())
    {
        tuples.push_back(lines[currentLine]);
        currentLine++;
    }
    return tuples;
}

SLTParser::Query SLTParser::expectQuery()
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    std::string query;
    bool firstLine = true;
    while (currentLine < lines.size())
    {
        if (!emptyOrComment(lines[currentLine]))
        {
            /// Query definition ends with result delimiter.
            if (lines[currentLine] == ResultDelimiter)
            {
                --currentLine;
                break;
            }
            if (!firstLine)
            {
                query += "\n";
            }
            query += lines[currentLine];
            firstLine = false;
        }
        currentLine++;
    }
    INVARIANT(!query.empty(), "when expecting a query keyword the query should not be empty");
    return query;
}
}
