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
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestState.hpp>

namespace
{

/// Parses the stream into a schema. It expects a string in the format: FIELDNAME FIELDTYPE, FIELDNAME FIELDTYPE, ...
NES::Systest::SystestParser::SystestSchema parseSchemaFields(const std::vector<std::string>& arguments)
{
    NES::Systest::SystestParser::SystestSchema schema;
    if (arguments.size() % 2 != 0)
    {
        if (const auto& lastArg = arguments.back(); lastArg.ends_with(".csv"))
        {
            throw NES::SLTUnexpectedToken(
                "Incomplete fieldtype/fieldname pair for arguments {}; {} potentially is a CSV file? Are you mixing semantics",
                fmt::join(arguments, ","),
                lastArg);
        }
        throw NES::SLTUnexpectedToken("Incomplete fieldtype/fieldname pair for arguments {}", fmt::join(arguments, ", "));
    }

    for (size_t i = 0; i < arguments.size(); i += 2)
    {
        NES::DataType dataType;
        if (auto type = magic_enum::enum_cast<NES::DataType::Type>(arguments[i]); type.has_value())
        {
            dataType = NES::DataTypeProvider::provideDataType(type.value());
        }
        else if (NES::Util::toLowerCase(arguments[i]) == "varsized")
        {
            dataType = NES::DataTypeProvider::provideDataType(NES::DataType::Type::VARSIZED);
        }
        else
        {
            throw NES::SLTUnexpectedToken("Unknown basic type: " + arguments[i]);
        }
        schema.emplace_back(dataType, arguments[i + 1]);
    }

    return schema;
}


bool emptyOrComment(const std::string& line)
{
    return line.empty() /// completely empty
        || line.find_first_not_of(" \t\n\r\f\v") == std::string::npos /// only whitespaces
        || line.starts_with('#'); /// slt comment
}
}

namespace NES::Systest
{

static constexpr auto CSVSourceToken = "SourceCSV"s;
static constexpr auto SLTSourceToken = "Source"s;
static constexpr auto QueryToken = "SELECT"s;
static constexpr auto SinkToken = "SINK"s;
static constexpr auto ResultDelimiter = "----"s;
static constexpr auto ErrorToken = "ERROR"s;

static const std::array stringToToken = std::to_array<std::pair<std::string_view, TokenType>>(
    {{CSVSourceToken, TokenType::CSV_SOURCE},
     {SLTSourceToken, TokenType::SLT_SOURCE},
     {QueryToken, TokenType::QUERY},
     {SinkToken, TokenType::SINK},
     {ResultDelimiter, TokenType::RESULT_DELIMITER},
     {ErrorToken, TokenType::ERROR_EXPECTATION}});

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

void SystestParser::registerOnSystestSinkCallback(SystestSinkCallback callback)
{
    this->onSystestSinkCallback = std::move(callback);
}

void SystestParser::registerOnCSVSourceCallback(CSVSourceCallback callback)
{
    this->onCSVSourceCallback = std::move(callback);
}

void SystestParser::registerOnErrorExpectationCallback(ErrorExpectationCallback callback)
{
    this->onErrorExpectationCallback = std::move(callback);
}

/// Here we model the structure of the test file by what we `expect` to see.
void SystestParser::parse()
{
    SystestQueryIdAssigner queryIdAssigner{};
    while (auto token = getNextToken())
    {
        switch (token.value())
        {
            case TokenType::CSV_SOURCE: {
                auto source = expectCSVSource();
                if (onCSVSourceCallback)
                {
                    onCSVSourceCallback(source);
                }
                break;
            }
            case TokenType::SLT_SOURCE: {
                auto source = expectSLTSource();
                if (onSLTSourceCallback)
                {
                    onSLTSourceCallback(source);
                }
                break;
            }
            case TokenType::SINK: {
                auto sink = expectSink();
                if (onSystestSinkCallback)
                {
                    onSystestSinkCallback(std::move(sink));
                }
                break;
            }
            case TokenType::QUERY: {
                if (onQueryCallback)
                {
                    onQueryCallback(expectQuery(), queryIdAssigner.getNextQueryNumber());
                }
                break;
            }
            case TokenType::RESULT_DELIMITER: {
                /// Look ahead for error expectation
                if (const auto optionalToken = peekToken(); optionalToken == TokenType::ERROR_EXPECTATION)
                {
                    ++currentLine;
                    queryIdAssigner.skipQueryResultOfQueryWithExpectedError();
                    auto expectation = expectError();
                    if (onErrorExpectationCallback)
                    {
                        onErrorExpectationCallback(std::move(expectation));
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
            case TokenType::INVALID:
                throw TestException(
                    "Should never run into the INVALID token during systest file parsing, but got line: {}.", lines[currentLine]);
            case TokenType::ERROR_EXPECTATION:
                throw TestException(
                    "Should never run into the ERROR_EXPECTATION token during systest file parsing, but got line: {}", lines[currentLine]);
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
    if (Util::toLowerCase(potentialToken).starts_with(Util::toLowerCase(QueryToken)))
    {
        return TokenType::QUERY;
    }
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

    std::string potentialToken;
    std::istringstream stream(lines[currentLine]);
    stream >> potentialToken;

    INVARIANT(!potentialToken.empty(), "a potential token should never be empty");

    return getTokenIfValid(potentialToken);
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

    std::string potentialToken;
    std::istringstream stream(lines[peekLine]);
    stream >> potentialToken;

    INVARIANT(!potentialToken.empty(), "a potential token should never be empty");
    return getTokenIfValid(potentialToken);
}

SystestParser::SystestSink SystestParser::expectSink() const
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    SystestSink sink;
    const auto& line = lines[currentLine];
    std::istringstream lineAsStream(line);

    /// Read and discard the first word as it is always Source
    std::string discard;
    if (!(lineAsStream >> discard))
    {
        throw SLTUnexpectedToken("failed to read the first word in: {}", line);
    }
    INVARIANT(
        Util::toLowerCase(discard) == Util::toLowerCase(SinkToken), "Expected first word to be `{}` for sink statement", SLTSourceToken);

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
    INVARIANT(
        Util::toLowerCase(discard) == Util::toLowerCase(SLTSourceToken),
        "Expected first word to be `{}` for source statement",
        SLTSourceToken);

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
    INVARIANT(
        Util::toLowerCase(discard) == Util::toLowerCase(CSVSourceToken),
        "Expected first word to be `{}` for csv source statement",
        CSVSourceToken);

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

std::vector<std::string> SystestParser::expectTuples(const bool ignoreFirst)
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist: {}", currentLine);
    std::vector<std::string> tuples;
    /// skip the result line `----`
    if (currentLine < lines.size() && (Util::toLowerCase(lines[currentLine]) == Util::toLowerCase(ResultDelimiter) || ignoreFirst))
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

std::string SystestParser::expectQuery()
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    std::string queryString;
    bool firstLine = true;
    while (currentLine < lines.size())
    {
        if (!emptyOrComment(lines[currentLine]))
        {
            /// Query definition ends with result delimiter.
            if (Util::toLowerCase(lines[currentLine]) == Util::toLowerCase(ResultDelimiter))
            {
                --currentLine;
                break;
            }
            if (!firstLine)
            {
                queryString += "\n";
            }
            queryString += lines[currentLine];
            firstLine = false;
        }
        currentLine++;
    }
    INVARIANT(!queryString.empty(), "when expecting a query keyword the queryString should not be empty");
    return queryString;
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
