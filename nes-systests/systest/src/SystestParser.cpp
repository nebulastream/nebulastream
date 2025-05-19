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
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <InputFormatters/InputFormatterProvider.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>

namespace
{

/// Parses the stream into a schema. It expects a string in the format: FIELDNAME FIELDTYPE, FIELDNAME FIELDTYPE, ...
NES::Systest::SystestSchema parseSchemaFields(const std::vector<std::string>& arguments)
{
    NES::Systest::SystestSchema schema;
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

std::optional<std::filesystem::path> validateYamlConfigPath(const std::string_view filePath)
{
    const std::filesystem::path path(filePath);
    if (not std::filesystem::exists(path) or not std::filesystem::is_regular_file(path) or not path.has_extension())
    {
        return std::nullopt;
    }

    const std::string ext = NES::Util::toLowerCase(path.extension().string());
    return (ext == ".yaml" or ext == ".yml") ? std::optional{path} : std::nullopt;
}

NES::SystestAttachSource validateAttachSource(const std::unordered_set<std::string>& seenLogicalSourceNames, const std::string& line)
{
    (void)seenLogicalSourceNames; /// allows release mode to build (fails because of 'unused-parameter' otherwise)
    const auto attachSourceTokens = NES::Util::splitWithStringDelimiter<std::string>(line, " ");
    /// Attach SourceType (SourceConfig) IFormatter (IFormatterConfig) LogicalSourceName DataIngestionType
    constexpr size_t minNumberOfTokensInAttachSource = 5;
    constexpr size_t maxNumberOfTokensInAttachSource = 7;

    /// Preliminary checks
    INVARIANT(
        attachSourceTokens.size() >= minNumberOfTokensInAttachSource and attachSourceTokens.size() <= maxNumberOfTokensInAttachSource,
        "Expected between {} and {} tokens for attach source, but found {} tokens in \"{}\"",
        minNumberOfTokensInAttachSource,
        maxNumberOfTokensInAttachSource,
        attachSourceTokens.size(),
        fmt::join(attachSourceTokens, ", "));
    INVARIANT(NES::Util::toUpperCase(attachSourceTokens.front()) == "ATTACH", "Expected first token of attach source to be 'ATTACH'");

    /// Validate and parse tokens
    size_t nextTokenIdx = 1;
    NES::SystestAttachSource attachSource{};
    INVARIANT(
        NES::Sources::SourceProvider::contains(attachSourceTokens.at(nextTokenIdx)),
        "Expected second token of attach source to be valid source type, but was: {}",
        attachSourceTokens.at(1));
    attachSource.sourceType = std::string(attachSourceTokens.at(nextTokenIdx++));

    attachSource.sourceConfigurationPath
        = [](const std::vector<std::string>& attachSourceTokens, const std::string_view sourceType, size_t& nextTokenIdx)
        {
            if (const auto sourceConfigPath = validateYamlConfigPath(attachSourceTokens.at(nextTokenIdx)))
            {
                ++nextTokenIdx;
                return sourceConfigPath.value();
            }
            /// Set default source config path
            return std::filesystem::path(TEST_CONFIGURATION_DIR) / fmt::format("sources/{}_default.yaml", NES::Util::toLowerCase(sourceType));
        }(attachSourceTokens, attachSource.sourceType, nextTokenIdx);

    INVARIANT(
        NES::InputFormatters::InputFormatterProvider::contains(attachSourceTokens.at(nextTokenIdx)),
        "Expected token after source config to be a valid input formatter, but was: {}",
        attachSourceTokens.at(nextTokenIdx));
    attachSource.inputFormatterType = attachSourceTokens.at(nextTokenIdx++);

    attachSource.inputFormatterConfigurationPath
        = [](const std::vector<std::string>& attachSourceTokens, const std::string_view inputFormatterType, size_t& nextTokenIdx)
        {
            if (const auto inputFormatterConfigPath = validateYamlConfigPath(attachSourceTokens.at(nextTokenIdx)))
            {
                ++nextTokenIdx;
                return inputFormatterConfigPath.value();
            }
            /// Set default source config path
            return std::filesystem::path(TEST_CONFIGURATION_DIR)
                / fmt::format("inputFormatters/{}_default.yaml", NES::Util::toLowerCase(inputFormatterType));
        }(attachSourceTokens, attachSource.inputFormatterType, nextTokenIdx);

    INVARIANT(
        seenLogicalSourceNames.contains(attachSourceTokens.at(nextTokenIdx)),
        "Expected second to last token of attach source to be an existing logical source name, but was: {}",
        attachSourceTokens.at(nextTokenIdx));
    attachSource.logicalSourceName = attachSourceTokens.at(nextTokenIdx++);

    INVARIANT(
        magic_enum::enum_cast<NES::TestDataIngestionType>(NES::Util::toUpperCase(attachSourceTokens.at(nextTokenIdx))),
        "Last keyword of attach source must be a valid TestDataIngestionType, but was: {}",
        attachSourceTokens.at(nextTokenIdx));
    attachSource.testDataIngestionType
        = magic_enum::enum_cast<NES::TestDataIngestionType>(NES::Util::toUpperCase(attachSourceTokens.at(nextTokenIdx++))).value();

    INVARIANT(
        nextTokenIdx == attachSourceTokens.size(),
        "Number of parsed tokens {} does not match number of input tokens {}",
        nextTokenIdx,
        attachSourceTokens.size());
    return attachSource;
}
}

namespace NES::Systest
{

static constexpr auto SLTSourceToken = "Source"s;
static constexpr auto AttachSourceToken = "Attach"s;
static constexpr auto QueryToken = "SELECT"s;
static constexpr auto SinkToken = "SINK"s;
static constexpr auto ResultDelimiter = "----"s;

static constexpr std::array<std::pair<std::string_view, TokenType>, 6> stringToToken
    = {{{SLTSourceToken, TokenType::SLT_SOURCE},
        {AttachSourceToken, TokenType::ATTACH_SOURCE},
        {QueryToken, TokenType::QUERY},
        {SinkToken, TokenType::SINK},
        {ResultDelimiter, TokenType::RESULT_DELIMITER}}};

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

void SystestParser::registerOnSLTSourceCallback(SLTSourceCallback callback)
{
    this->onSLTSourceCallback = std::move(callback);
}
void SystestParser::registerOnAttachSourceCallback(AttachSourceCallback callback)
{
    this->onAttachSourceCallback = std::move(callback);
}

void SystestParser::registerOnSystestSystestSinkCallback(SystestSinkCallback callback)
{
    this->onSystestSinkCallback = std::move(callback);
}

void SystestParser::parse(SystestStarterGlobals& systestStarterGlobals, const std::string_view testFileName)
{
    SystestQueryNumberAssigner queryNumberAssigner{};
    while (auto token = nextToken())
    {
        switch (token.value())
        {
            case TokenType::ATTACH_SOURCE: {
                if (onAttachSourceCallback)
                {
                    onAttachSourceCallback(expectAttachSource());
                }
                break;
            }
            case TokenType::SLT_SOURCE: {
                auto source = expectSLTSource();
                if (onSLTSourceCallback)
                {
                    onSLTSourceCallback(std::move(source));
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
                    onQueryCallback(expectQuery(), queryNumberAssigner.getNextQueryNumber());
                }
                break;
            }
            case TokenType::RESULT_DELIMITER: {
                systestStarterGlobals.addQueryResult(testFileName, queryNumberAssigner.getNextQueryResultNumber(), expectTuples());
                break;
            }
            case TokenType::INVALID: {
                throw SLTUnexpectedToken("got invalid token in line: {}", lines[currentLine]);
            }
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
    seenLogicalSourceNames.emplace(source.name);

    return source;
}

/// Attach SOURCE_TYPE LOGICAL_SOURCE_NAME DATA_SOURCE_TYPE
/// Attach SOURCE_TYPE SOURCE_CONFIG_PATH LOGICAL_SOURCE_NAME DATA_SOURCE_TYPE
SystestAttachSource SystestParser::expectAttachSource()
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    switch (auto attachSource = validateAttachSource(seenLogicalSourceNames, lines[currentLine]); attachSource.testDataIngestionType)
    {
        case TestDataIngestionType::INLINE: {
            attachSource.tuples = {expectTuples(true)};
            return attachSource;
        }
        case TestDataIngestionType::FILE: {
            attachSource.fileDataPath = {expectFilePath()};
            return attachSource;
        }
    }
}

std::filesystem::path SystestParser::expectFilePath()
{
    ++currentLine;
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    if (const auto parsedFilePath = std::filesystem::path(lines.at(currentLine));
        std::filesystem::exists(parsedFilePath) and parsedFilePath.has_filename())
    {
        return parsedFilePath;
    }
    throw TestException("Attach source with FileData must be followed by valid file path, but got: {}", lines.at(currentLine));
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
            if (lines[currentLine] == ResultDelimiter)
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
}
