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

std::vector<NES::Systest::ConfigurationOverride> parseConfigurationLine(const std::string& line, std::string_view kindLabel)
{
    std::istringstream stream(line);

    std::string token;
    std::string key;
    stream >> token >> key;

    if (!key.ends_with(':'))
    {
        throw NES::SLTUnexpectedToken("Expected colon at end of key: '{}'", key);
    }
    key.pop_back(); /// remove trailing ':'

    if (key.empty())
    {
        throw NES::SLTUnexpectedToken("Expected {} key before colon, but got empty key", kindLabel);
    }

    std::string valueList;
    std::getline(stream >> std::ws, valueList);

    if (valueList.empty())
    {
        throw NES::SLTUnexpectedToken("Expected {} value after key '{}', but got empty value", kindLabel, key);
    }

    std::vector<std::string> values;
    auto invalidFormat = [&](std::string_view details)
    { throw NES::SLTUnexpectedToken("Invalid {} format for key '{}': '{}'. {}", kindLabel, key, valueList, details); };

    if (valueList.front() == '[' && valueList.back() == ']')
    {
        valueList = valueList.substr(1, valueList.size() - 2);
        if (valueList.empty())
        {
            invalidFormat("Expected at least one value inside the brackets");
        }
        values = NES::Util::splitWithStringDelimiter<std::string>(valueList, ",");
    }
    else
    {
        if (valueList.find_first_of("[]") != std::string::npos)
        {
            invalidFormat("Use either single value or properly formatted list in square brackets");
        }
        if (valueList.find(',') != std::string::npos)
        {
            invalidFormat("Multiple values must be enclosed in square brackets");
        }
        values = {valueList};
    }

    std::vector<NES::Systest::ConfigurationOverride> result;
    for (auto& value : values)
    {
        value = NES::Util::trimWhiteSpaces(value);
        if (value.empty())
        {
            throw NES::SLTUnexpectedToken("Empty {} value found for key '{}'", kindLabel, key);
        }
        NES::Systest::ConfigurationOverride override;
        override.overrideParameters[key] = value;
        result.emplace_back(std::move(override));
    }
    return result;
}

}

namespace NES::Systest
{

static constexpr auto SystestLogicalSourceToken = "Source"s;
static constexpr auto AttachSourceToken = "Attach"s;
static constexpr auto ModelToken = "MODEL"sv;
static constexpr auto QueryToken = "SELECT"s;
static constexpr auto SinkToken = "SINK"s;
static constexpr auto ResultDelimiter = "----"s;
static constexpr auto ErrorToken = "ERROR"s;

static const std::array stringToToken = std::to_array<std::pair<std::string_view, TokenType>>(
    {{SystestLogicalSourceToken, TokenType::LOGICAL_SOURCE},
     {AttachSourceToken, TokenType::ATTACH_SOURCE},
     {QueryToken, TokenType::QUERY},
     {SinkToken, TokenType::SINK},
     {ModelToken, TokenType::MODEL},
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

void SystestParser::registerOnSystestLogicalSourceCallback(SystestLogicalSourceCallback callback)
{
    this->onSystestLogicalSourceCallback = std::move(callback);
}

void SystestParser::registerOnSystestAttachSourceCallback(SystestAttachSourceCallback callback)
{
    this->onAttachSourceCallback = std::move(callback);
}

void SystestParser::registerOnModelCallback(ModelCallback callback)
{
    this->onModelCallback = std::move(callback);
}

void SystestParser::registerOnSystestSinkCallback(SystestSinkCallback callback)
{
    this->onSystestSinkCallback = std::move(callback);
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
            case TokenType::ATTACH_SOURCE: {
                if (onAttachSourceCallback)
                {
                    onAttachSourceCallback(expectAttachSource());
                }
                break;
            }
            case TokenType::MODEL: {
                auto model = expectModel();
                if (onModelCallback)
                {
                    onModelCallback(std::move(model));
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
    return TokenType::INVALID;
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

Nebuli::Inference::ModelDescriptor SystestParser::expectModel()
{
    try
    {
        if (lines.size() < currentLine + 2)
        {
            throw SLTUnexpectedToken("expected at least three lines for model definition.");
        }

        Nebuli::Inference::ModelDescriptor model;
        auto& modelNameLine = lines[currentLine];
        auto _ = moveToNextToken();
        auto& inputLine = lines[currentLine];
        _ = moveToNextToken();
        auto& outputLine = lines[currentLine];
        _ = moveToNextToken();
        auto& batchSizeLine = lines[currentLine];

        std::istringstream stream(modelNameLine);
        std::string discard;
        if (!(stream >> discard))
        {
            throw SLTUnexpectedToken("failed to read the first word in: {}", modelNameLine);
        }
        if (!(stream >> model.name))
        {
            throw SLTUnexpectedToken("failed to read model name in {}", modelNameLine);
        }
        if (!(stream >> model.path))
        {
            throw SLTUnexpectedToken("failed to read model path in {}", modelNameLine);
        }

        auto inputTypeNames = NES::Util::splitWithStringDelimiter<std::string>(inputLine, " ");
        auto types = std::views::transform(inputTypeNames, [](const auto& typeName) { return DataTypeProvider::provideDataType(typeName); })
            | std::ranges::to<std::vector>();
        model.inputs = types;

        auto outputSchema = NES::Util::splitWithStringDelimiter<std::string>(outputLine, " ");

        for (auto [type, name] : parseSchemaFields(outputSchema))
        {
            model.outputs.addField(name, type);
        }

        auto batchSize = NES::Util::splitWithStringDelimiter<std::string>(batchSizeLine, " ");
        model.batchSize = static_cast<size_t>(std::stoull(batchSize[1]));

        return model;
    }
    catch (Exception& e)
    {
        auto modelParserSchema = "MODEL <model_name> <model_path>"
                                 "<type-0> ... <type-N>"
                                 "<type-0> <output-name-0> ... <type-N> <output-name-N>"
                                 "BATCH_SIZE <batch_size>"sv;
        e.what() += fmt::format("\nWhen Parsing a Model Statement:\n{}", modelParserSchema);
        throw;
    }
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
