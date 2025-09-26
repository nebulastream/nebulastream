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
#include <SystestSources/SourceTypes.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
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

NES::SystestAttachSource parseAttachSource(const std::string& line)
{
    const auto attachSourceTokens = NES::Util::splitWithStringDelimiter<std::string>(line, " ");
    /// Attach SourceType (SourceConfig) IFormatter (IFormatterConfig) LogicalSourceName DataIngestionType
    constexpr size_t minNumberOfTokensInAttachSource = 5;
    constexpr size_t maxNumberOfTokensInAttachSource = 7;

    /// Preliminary checks
    if (attachSourceTokens.size() < minNumberOfTokensInAttachSource or attachSourceTokens.size() > maxNumberOfTokensInAttachSource)
    {
        throw NES::SLTUnexpectedToken(
            "Expected between {} and {} tokens for attach source, but found {} tokens in \"{}\"",
            minNumberOfTokensInAttachSource,
            maxNumberOfTokensInAttachSource,
            attachSourceTokens.size(),
            fmt::join(attachSourceTokens, ", "));
    }
    if (NES::Util::toUpperCase(attachSourceTokens.front()) != "ATTACH")
    {
        throw NES::SLTUnexpectedToken("Expected first token of attach source to be 'ATTACH'");
    }

    /// Validate and parse tokens
    size_t nextTokenIdx = 1;
    NES::SystestAttachSource attachSource{};

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

    attachSource.logicalSourceName = attachSourceTokens.at(nextTokenIdx++);

    if (not magic_enum::enum_cast<NES::TestDataIngestionType>(NES::Util::toUpperCase(attachSourceTokens.at(nextTokenIdx))))
    {
        throw NES::SLTUnexpectedToken(
            "Last keyword of attach source must be a valid TestDataIngestionType, but was: {}", attachSourceTokens.at(nextTokenIdx));
    }
    attachSource.testDataIngestionType
        = magic_enum::enum_cast<NES::TestDataIngestionType>(NES::Util::toUpperCase(attachSourceTokens.at(nextTokenIdx++))).value();

    if (nextTokenIdx != attachSourceTokens.size())
    {
        throw NES::SLTUnexpectedToken(
            "Number of parsed tokens {} does not match number of input tokens {}", nextTokenIdx, attachSourceTokens.size());
    }
    return attachSource;
}
}

namespace NES::Systest
{

static constexpr auto SystestLogicalSourceToken = "Source"s;
static constexpr auto AttachSourceToken = "Attach"s;
static constexpr auto QueryToken = "SELECT"s;
static constexpr auto SinkToken = "SINK"s;
static constexpr auto ResultDelimiter = "----"s;
static constexpr auto ErrorToken = "ERROR"s;
static constexpr auto DifferentialToken = "===="s;

static const std::array stringToToken = std::to_array<std::pair<std::string_view, TokenType>>(
    {{SystestLogicalSourceToken, TokenType::LOGICAL_SOURCE},
     {AttachSourceToken, TokenType::ATTACH_SOURCE},
     {QueryToken, TokenType::QUERY},
     {SinkToken, TokenType::SINK},
     {ResultDelimiter, TokenType::RESULT_DELIMITER},
     {ErrorToken, TokenType::ERROR_EXPECTATION},
     {DifferentialToken, TokenType::DIFFERENTIAL}});

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

void SystestParser::registerOnSystestLogicalSourceCallback(SystestLogicalSourceCallback callback)
{
    this->onSystestLogicalSourceCallback = std::move(callback);
}

void SystestParser::registerOnSystestAttachSourceCallback(SystestAttachSourceCallback callback)
{
    this->onAttachSourceCallback = std::move(callback);
}

void SystestParser::registerOnSystestSinkCallback(SystestSinkCallback callback)
{
    this->onSystestSinkCallback = std::move(callback);
}

void SystestParser::registerOnErrorExpectationCallback(ErrorExpectationCallback callback)
{
    this->onErrorExpectationCallback = std::move(callback);
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
            case TokenType::ATTACH_SOURCE: {
                if (onAttachSourceCallback)
                {
                    onAttachSourceCallback(expectAttachSource());
                }
                break;
            }
            case TokenType::LOGICAL_SOURCE: {
                auto [logicalSource, attachSourceOpt] = expectSystestLogicalSource();
                if (onSystestLogicalSourceCallback)
                {
                    onSystestLogicalSourceCallback(logicalSource);
                }
                if (onAttachSourceCallback and attachSourceOpt.has_value())
                {
                    onAttachSourceCallback(std::move(attachSourceOpt.value()));
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
                static const std::unordered_set<TokenType> defaultQueryStopTokens{
                    TokenType::RESULT_DELIMITER,
                    TokenType::DIFFERENTIAL,
                    TokenType::LOGICAL_SOURCE,
                    TokenType::ATTACH_SOURCE,
                    TokenType::SINK};

                auto query = expectQuery(defaultQueryStopTokens);
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

            case TokenType::INVALID:
                throw SLTUnexpectedToken(
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

    std::string potentialToken;
    std::istringstream stream(lines[currentLine]);
    stream >> potentialToken;

    INVARIANT(!potentialToken.empty(), "a potential token should never be empty");

    if (auto token = getTokenIfValid(potentialToken); token.has_value())
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
        Util::toLowerCase(discard) == Util::toLowerCase(SinkToken),
        "Expected first word to be `{}` for sink statement",
        SystestLogicalSourceToken);

    /// Read the source name and check if successful
    if (!(lineAsStream >> sink.name))
    {
        throw SLTUnexpectedToken("failed to read sink name in {}", line);
    }

    std::string sinkTypeTokenOrFieldType;
    std::vector<std::string> arguments;
    if (!(lineAsStream >> sinkTypeTokenOrFieldType))
    {
        throw SLTUnexpectedToken("failed to read sink name or type token in {}", line);
    }
    if (sinkTypeTokenOrFieldType == "TYPE")
    {
        /// Read the sink type and check if successful
        if (!(lineAsStream >> sink.type))
        {
            throw SLTUnexpectedToken("failed to read sink type in {}", line);
        }
    }
    else
    {
        sink.type = "File";
        arguments.push_back(sinkTypeTokenOrFieldType);
    }

    std::string argument;
    while (lineAsStream >> argument)
    {
        arguments.push_back(argument);
    }

    /// After the source definition line we expect schema fields
    sink.fields = parseSchemaFields(arguments);

    return sink;
}

std::pair<SystestParser::SystestLogicalSource, std::optional<SystestAttachSource>>
SystestParser::expectInlineGeneratorSource(SystestLogicalSource& source, const std::vector<std::string>& attachSourceTokens)
{
    std::vector<std::string> arguments;
    auto curPos = attachSourceTokens.begin();
    std::advance(curPos, 2); /// First two words are always: Source sourceName
    for (; curPos != attachSourceTokens.end(); ++curPos)
    {
        if (magic_enum::enum_cast<TestDataIngestionType>(Util::toUpperCase(*curPos)) == TestDataIngestionType::GENERATOR)
        {
            break;
        }
        arguments.emplace_back(*curPos);
    }
    source.fields = parseSchemaFields(arguments);
    std::unordered_map<std::string, std::string> configOptions;
    INVARIANT(
        std::distance(curPos, attachSourceTokens.end()) % 2 != 0,
        "Generator Config Options are always pairwise! Error in {}",
        lines[currentLine]);
    ++curPos;
    while (curPos != attachSourceTokens.end())
    {
        std::string key = *curPos;
        std::string value = *(++curPos);
        value = value.ends_with(',') ? value.substr(0, value.size() - 1) : value;

        configOptions.try_emplace(key, value);
        ++curPos;
    }
    currentLine++;
    std::vector<std::string> fieldSchemas;
    while (!emptyOrComment(lines[currentLine]))
    {
        fieldSchemas.emplace_back(lines[currentLine]);
        currentLine++;
    }
    return std::make_pair(
        source,
        SystestAttachSource{
            .sourceType = "Generator",
            .sourceConfigurationPath = "inline:///",
            .inputFormatterType = "CSV",
            .inputFormatterConfigurationPath = std::filesystem::path(TEST_CONFIGURATION_DIR) / "inputFormatters/csv_default.yaml",
            .logicalSourceName = source.name,
            .testDataIngestionType = TestDataIngestionType::GENERATOR,
            .tuples = {},
            .fileDataPath = {},
            .serverThreads = nullptr,
            .inlineGeneratorConfiguration = InlineGeneratorConfiguration{.fieldSchema = fieldSchemas, .options = configOptions}});
}

std::pair<SystestParser::SystestLogicalSource, std::optional<SystestAttachSource>> SystestParser::expectSystestLogicalSource()
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    SystestLogicalSource source;
    auto& line = lines[currentLine];
    const auto attachSourceTokens = Util::splitWithStringDelimiter<std::string>(line, " ");

    /// Read and discard the first word as it is always Source
    if (attachSourceTokens.front() != SystestLogicalSourceToken)
    {
        throw SLTUnexpectedToken("failed to read the first word in: {}", line);
    }

    /// Read the source name and check if successful
    if (attachSourceTokens.size() <= 1)
    {
        throw SLTUnexpectedToken("failed to read source name in {}", line);
    }
    source.name = attachSourceTokens.at(1);

    if (const auto dataIngestionType = magic_enum::enum_cast<TestDataIngestionType>(Util::toUpperCase(attachSourceTokens.back())))
    {
        const std::vector<std::string> arguments = attachSourceTokens | std::views::drop(2)
            | std::views::take(std::ranges::size(attachSourceTokens) - 3) | std::ranges::to<std::vector<std::string>>();

        /// After the source definition line we expect schema fields
        source.fields = parseSchemaFields(arguments);

        const auto attachSource = [&]()
        {
            switch (dataIngestionType.value())
            {
                case TestDataIngestionType::INLINE: {
                    ++currentLine; /// proceed to results
                    return SystestAttachSource{
                        .sourceType = "File",
                        .sourceConfigurationPath = std::filesystem::path(TEST_CONFIGURATION_DIR) / "sources/file_default.yaml",
                        .inputFormatterType = "CSV",
                        .inputFormatterConfigurationPath
                        = std::filesystem::path(TEST_CONFIGURATION_DIR) / "inputFormatters/csv_default.yaml",
                        .logicalSourceName = source.name,
                        .testDataIngestionType = dataIngestionType.value(),
                        .tuples = expectTuples(false),
                        .fileDataPath = {},
                        .serverThreads = nullptr,
                        .inlineGeneratorConfiguration = std::nullopt};
                }
                case TestDataIngestionType::FILE: {
                    return SystestAttachSource{
                        .sourceType = "File",
                        .sourceConfigurationPath = std::filesystem::path(TEST_CONFIGURATION_DIR) / "sources/file_default.yaml",
                        .inputFormatterType = "CSV",
                        .inputFormatterConfigurationPath
                        = std::filesystem::path(TEST_CONFIGURATION_DIR) / "inputFormatters/csv_default.yaml",
                        .logicalSourceName = source.name,
                        .testDataIngestionType = dataIngestionType.value(),
                        .tuples = {},
                        .fileDataPath = expectFilePath(),
                        .serverThreads = nullptr,
                        .inlineGeneratorConfiguration = std::nullopt};
                }
                case TestDataIngestionType::GENERATOR: {
                    return SystestAttachSource{
                        .sourceType = "Generator",
                        .sourceConfigurationPath = expectFilePath(),
                        .inputFormatterType = "CSV",
                        .inputFormatterConfigurationPath
                        = std::filesystem::path(TEST_CONFIGURATION_DIR) / "inputFormatters/csv_default.yaml",
                        .logicalSourceName = source.name,
                        .testDataIngestionType = dataIngestionType.value(),
                        .tuples = {},
                        .fileDataPath = {},
                        .serverThreads = nullptr,
                        .inlineGeneratorConfiguration = std::nullopt};
                }
            }
            std::unreachable();
        }();
        return std::make_pair(source, attachSource);
    }
    if (std::ranges::any_of(
            attachSourceTokens
                | std::views::transform([](const auto& token) { return magic_enum::enum_cast<TestDataIngestionType>(token); })
                | std::views::filter([](const auto& optType) { return optType.has_value(); }),
            [](const auto& optType) { return optType.value() == TestDataIngestionType::GENERATOR; }))
    {
        return expectInlineGeneratorSource(source, attachSourceTokens);
    }


    const std::vector<std::string> arguments = attachSourceTokens | std::views::drop(2) | std::ranges::to<std::vector<std::string>>();

    /// After the source definition line we expect schema fields
    source.fields = parseSchemaFields(arguments);

    return std::make_pair(source, std::nullopt);
}

/// Attach SOURCE_TYPE LOGICAL_SOURCE_NAME DATA_SOURCE_TYPE
/// Attach SOURCE_TYPE SOURCE_CONFIG_PATH LOGICAL_SOURCE_NAME DATA_SOURCE_TYPE
SystestAttachSource SystestParser::expectAttachSource()
{
    INVARIANT(currentLine < lines.size(), "current parse line should exist");

    switch (auto attachSource = parseAttachSource(lines[currentLine]); attachSource.testDataIngestionType)
    {
        case TestDataIngestionType::INLINE: {
            attachSource.tuples = {expectTuples(true)};
            return attachSource;
        }
        case TestDataIngestionType::FILE: {
            attachSource.fileDataPath = {expectFilePath()};
            return attachSource;
        }
        case TestDataIngestionType::GENERATOR: {
            attachSource.sourceConfigurationPath = {expectFilePath()};
            return attachSource;
        }
    }
    std::unreachable();
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
        TokenType::LOGICAL_SOURCE,
        TokenType::ATTACH_SOURCE,
        TokenType::SINK,
        TokenType::RESULT_DELIMITER,
        TokenType::DIFFERENTIAL,
        TokenType::ERROR_EXPECTATION};

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
