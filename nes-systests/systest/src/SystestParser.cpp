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
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

namespace NES::Systest
{
static constexpr auto CSVSourceToken = "SourceCSV"s;
static constexpr auto SLTSourceToken = "Source"s;
static constexpr auto QueryToken = "SELECT"s;
static constexpr auto SinkToken = "SINK"s;
static constexpr auto ResultDelimiter = "----"s;

static const std::array<std::pair<std::string_view, TokenType>, 5> stringToToken
    = {{{CSVSourceToken, TokenType::CSV_SOURCE},
        {SLTSourceToken, TokenType::SLT_SOURCE},
        {QueryToken, TokenType::QUERY},
        {SinkToken, TokenType::SINK},
        {ResultDelimiter, TokenType::RESULT_DELIMITER}}};

static bool emptyOrComment(const std::string& line)
{
    return line.empty() /// completely empty
        || line.find_first_not_of(" \t\n\r\f\v") == std::string::npos /// only whitespaces
        || line.starts_with('#'); /// slt comment
}

/// Parses the stream into a schema. It expects a string in the format: FIELDNAME FIELDTYPE, FIELDNAME FIELDTYPE, ...
SystestParser::Schema parseSchemaFields(const std::vector<std::string>& arguments)
{
    SystestParser::Schema schema;
    if (arguments.size() % 2 != 0)
    {
        if (const auto& lastArg = arguments.back(); lastArg.ends_with(".csv"))
        {
            throw SLTUnexpectedToken(
                "Incomplete fieldtype/fieldname pair for arguments {}; {} potentially is a CSV file? Are you mixing semantics",
                fmt::join(arguments, ","),
                lastArg);
        }
        throw SLTUnexpectedToken("Incomplete fieldtype/fieldname pair for arguments {}", fmt::join(arguments, ", "));
    }

    for (size_t i = 0; i < arguments.size(); i += 2)
    {
        std::shared_ptr<DataType> dataType;
        if (auto type = magic_enum::enum_cast<BasicType>(arguments[i]); type.has_value())
        {
            dataType = DataTypeProvider::provideBasicType(type.value());
        }
        else if (NES::Util::toLowerCase(arguments[i]) == "varsized")
        {
            dataType = DataTypeProvider::provideDataType(LogicalType::VARSIZED);
        }
        else
        {
            throw SLTUnexpectedToken("Unknown basic type: " + arguments[i]);
        }
        schema.emplace_back(dataType, arguments[i + 1]);
    }

    return schema;
}

void SystestParser::registerSubstitutionRule(const SubstitutionRule& rule)
{
    auto found = std::ranges::find_if(substitutionRules, [&rule](const SubstitutionRule& r) { return r.keyword == rule.keyword; });
    PRECONDITION(
        found == substitutionRules.end(),
        "substitution rule keywords must be unique. Tried to register for the second time: {}",
        rule.keyword);
    substitutionRules.emplace_back(rule);
}

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
            /// Apply subsitutions & add to parsing lines
            applySubstitutionRules(line);
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

void SystestParser::registerOnSLTSourceCallback(SLTSourceCallback callback)
{
    this->onSLTSourceCallback = std::move(callback);
}

void SystestParser::registerOnSinkCallBack(SinkCallback callback)
{
    this->onSinkCallback = std::move(callback);
}

void SystestParser::registerOnCSVSourceCallback(CSVSourceCallback callback)
{
    this->onCSVSourceCallback = std::move(callback);
}

/// Here we model the structure of the test file by what we `expect` to see.
/// If we encounter something unexpected, we return false.
void SystestParser::parse()
{
    while (auto token = nextToken())
    {
        if (token == TokenType::CSV_SOURCE)
        {
            auto source = expectCSVSource();
            if (onCSVSourceCallback)
            {
                onCSVSourceCallback(std::move(source));
            }
        }
        else if (token == TokenType::SLT_SOURCE)
        {
            auto source = expectSLTSource();
            if (onSLTSourceCallback)
            {
                onSLTSourceCallback(std::move(source));
            }
        }
        else if (token == TokenType::SINK)
        {
            auto sink = expectSink();
            if (onSinkCallback)
            {
                onSinkCallback(std::move(sink));
            }
        }
        else if (token == TokenType::QUERY)
        {
            auto query = expectQuery();
            if (onQueryCallback)
            {
                onQueryCallback(std::move(query));
            }
            token = nextToken();
            if (token == TokenType::RESULT_DELIMITER)
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
        else if (token == TokenType::RESULT_DELIMITER)
        {
            throw SLTUnexpectedToken("unexpected occurrence of result delimiter `{}`", ResultDelimiter);
        }
        else if (token == TokenType::INVALID)
        {
            throw SLTUnexpectedToken("got invalid token in line: {}", lines[currentLine]);
        }
    }
}

void SystestParser::applySubstitutionRules(std::string& line)
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

std::optional<TokenType> SystestParser::getTokenIfValid(std::string potentialToken)
{
    /// Query is a special case as it's identifying token is not space seperated
    if (potentialToken.starts_with(QueryToken))
    {
        return TokenType::QUERY;
    }
    /// Lookup in map
    const auto* it = std::ranges::find_if(stringToToken, [&potentialToken](const auto& pair) { return pair.first == potentialToken; });
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

std::optional<TokenType> SystestParser::nextToken()
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

SystestParser::Sink SystestParser::expectSink() const
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    Sink sink;
    const auto& line = lines[currentLine];
    std::istringstream lineAsStream(line);

    /// Read and discard the first word as it is always Source
    std::string discard;
    if (!(lineAsStream >> discard))
    {
        throw SLTUnexpectedToken("failed to read the first word in: {}", line);
    }
    INVARIANT(discard == SinkToken, "Expected first word to be `{}` for sink statement", SLTSourceToken);

    /// Read the source name and check if successful
    if (!(lineAsStream >> sink.name))
    {
        throw SLTUnexpectedToken("failed to read sink name in {}", line);
    }

    std::vector<std::string> arguments;
    std::string argument;
    while (lineAsStream >> argument)
    {
        arguments.push_back(argument);
    }

    /// After the source definition line we expect schema fields
    sink.fields = parseSchemaFields(arguments);

    return sink;
}

SystestParser::SLTSource SystestParser::expectSLTSource()
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
        throw SLTUnexpectedToken("failed to read source name in {}", line);
    }

    std::vector<std::string> arguments;
    std::string argument;
    while (stream >> argument)
    {
        arguments.push_back(argument);
    }

    /// After the source definition line we expect schema fields
    source.fields = parseSchemaFields(arguments);

    /// After the source definition line, we expect the tuples
    source.tuples = expectTuples(true);

    return source;
}

SystestParser::CSVSource SystestParser::expectCSVSource() const
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");
    CSVSource source;
    const auto& line = lines[currentLine];
    std::istringstream stream(line);

    /// Read and discard the first word as it is always CSVSource
    std::string discard;
    if (!(stream >> discard))
    {
        throw SLTUnexpectedToken("failed to read the first word in: " + line);
    }
    INVARIANT(discard == CSVSourceToken, "Expected first word to be `{}` for csv source statement", CSVSourceToken);

    /// Read the source name and check if successful
    if (!(stream >> source.name))
    {
        throw SLTUnexpectedToken("failed to read source name in: " + line);
    }

    std::vector<std::string> arguments;
    std::string argument;
    while (stream >> argument)
    {
        arguments.push_back(argument);
    }

    source.csvFilePath = arguments.back();
    arguments.pop_back();

    source.fields = parseSchemaFields(arguments);
    return source;
}

SystestParser::ResultTuples SystestParser::expectTuples(const bool ignoreFirst)
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

SystestParser::Query SystestParser::expectQuery()
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
