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
#include <cctype>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <ranges>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace
{

bool emptyOrComment(const std::string& line)
{
    return line.empty() /// completely empty
        || line.find_first_not_of(" \t\n\r\f\v") == std::string::npos /// only whitespaces
        || line.starts_with('#'); /// slt comment
}

std::pair<std::string, std::vector<std::string>> parseConfigurationLine(const std::string& line, std::string_view kindLabel)
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
        values = NES::splitWithStringDelimiter<std::string>(valueList, ",");
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

    for (auto& value : values)
    {
        value = NES::trimWhiteSpaces(value);
        if (value.empty())
        {
            throw NES::SLTUnexpectedToken("Empty {} value found for key '{}'", kindLabel, key);
        }
    }
    return {std::move(key), std::move(values)};
}

}

namespace NES::Systest
{

using namespace std::string_view_literals;

static constexpr std::string_view CreateToken = "CREATE"sv;
static constexpr std::string_view QueryToken = "SELECT"sv;
static constexpr std::string_view ExplainToken = "EXPLAIN"sv;
static constexpr std::string_view ResultDelimiter = "----"sv;
static constexpr std::string_view ErrorToken = "ERROR"sv;
static constexpr std::string_view DifferentialToken = "===="sv;
static constexpr std::string_view ConfigurationToken = "CONFIGURATION"sv;
static constexpr std::string_view GlobalConfigurationToken = "GLOBALCONFIGURATION"sv;
static constexpr std::string_view SequentialExecutionToken = "SEQUENTIAL_EXECUTION"sv;

static const std::array stringToToken = std::to_array<std::pair<std::string_view, TokenType>>(
    {{CreateToken, TokenType::CREATE},
     {QueryToken, TokenType::QUERY},
     {ExplainToken, TokenType::EXPLAIN},
     {ResultDelimiter, TokenType::RESULT_DELIMITER},
     {ErrorToken, TokenType::ERROR_EXPECTATION},
     {ConfigurationToken, TokenType::CONFIGURATION},
     {GlobalConfigurationToken, TokenType::GLOBAL_CONFIGURATION},
     {DifferentialToken, TokenType::DIFFERENTIAL},
     {SequentialExecutionToken, TokenType::SEQUENTIAL_EXECUTION}});

SystestQueryId SystestQueryIdAssigner::getNextQueryNumber()
{
    verifyComplete();
    return SystestQueryId(currentQueryNumber++);
}

SystestQueryId SystestQueryIdAssigner::getNextQueryResultNumber()
{
    if (currentQueryNumber != (currentQueryResultNumber + 1))
    {
        throw SLTUnexpectedToken(
            "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
    }

    return SystestQueryId(currentQueryResultNumber++);
}

void SystestQueryIdAssigner::verifyComplete() const
{
    if (currentQueryNumber != currentQueryResultNumber)
    {
        throw SLTUnexpectedToken(
            "The number of queries {} must match the number of results {}", currentQueryNumber, currentQueryResultNumber);
    }
}

void SystestParser::registerSubstitutionRule(const SubstitutionRule& rule)
{
    PRECONDITION(!rule.keyword.empty(), "substitution rule keywords must not be empty");
    auto found
        = std::ranges::find_if(substitutionRules, [&rule](const SubstitutionRule& existing) { return existing.keyword == rule.keyword; });
    PRECONDITION(
        found == substitutionRules.end(),
        "substitution rule keywords must be unique. Tried to register for the second time: {}",
        rule.keyword);
    substitutionRules.emplace_back(rule);
}

/// We do not load the file in a constructor, as we want to be able to handle errors
bool SystestParser::loadFile(const std::filesystem::path& filePath)
{
    return loadFile(filePath, filePath.filename());
}

bool SystestParser::loadFile(const std::filesystem::path& filePath, const std::filesystem::path& relativeTestFile)
{
    std::ifstream infile(filePath);
    if (!infile.is_open() || infile.bad())
    {
        return false;
    }
    std::stringstream buffer;
    buffer << infile.rdbuf();
    if (!loadString(buffer.str()))
    {
        return false;
    }
    sourceFile = std::filesystem::weakly_canonical(filePath);
    this->relativeTestFile = relativeTestFile;
    return true;
}

bool SystestParser::loadString(const std::string& str)
{
    currentLine = 0;
    firstToken = true;
    shouldRevisitCurrentLine = false;
    lastParsedQuery.reset();
    lastParsedQueryId.reset();
    expectedResultType = ResultType::TUPLES;
    lines.clear();
    sourceLineNumbers.clear();
    sourceFile.clear();
    relativeTestFile.clear();

    std::istringstream stream(str);
    std::string line;
    size_t sourceLine = 0;
    while (std::getline(stream, line))
    {
        ++sourceLine;
        /// Remove commented code
        const size_t commentPos = line.find('#');
        if (commentPos != std::string::npos)
        {
            line = line.substr(0, commentPos);
        }
        /// add lines that do not start with a comment
        if (commentPos != 0)
        {
            applySubstitutionRules(line);
            /// Add to parsing lines
            lines.push_back(line);
            sourceLineNumbers.push_back(sourceLine);
        }
    }
    return true;
}

/// Here we model the structure of the test file by what we `expect` to see.
ParsedTestFile SystestParser::parse()
{
    static const std::unordered_set<TokenType> DefaultQueryStopTokens{TokenType::RESULT_DELIMITER, TokenType::DIFFERENTIAL};

    const auto physicalLine = [&](const size_t index) -> size_t
    {
        if (sourceLineNumbers.empty())
        {
            return 0;
        }
        return sourceLineNumbers.at(std::min(index, sourceLineNumbers.size() - 1));
    };
    ParsedTestFile parsedTestFile{.file = sourceFile, .relativeTestFile = relativeTestFile, .fixtures = {}, .cases = {}};
    std::vector<ConfigurationDirective> localConfiguration;
    std::vector<ConfigurationDirective> globalConfiguration;
    const auto findParsedCase = [&](const SystestQueryId id) -> ParsedCase&
    {
        auto parsedCase
            = std::ranges::find_if(parsedTestFile.cases, [&](const ParsedCase& candidate) { return candidate.key.queryNumber == id; });
        INVARIANT(parsedCase != parsedTestFile.cases.end(), "Parsed case {} must exist before its expectation", id);
        return *parsedCase;
    };
    const auto activeConfiguration = [&]
    {
        auto configuration = globalConfiguration;
        configuration.insert(configuration.end(), localConfiguration.begin(), localConfiguration.end());
        return configuration;
    };

    SystestQueryIdAssigner queryIdAssigner{};
    std::optional<SystestQueryId> previousCaseId;
    bool sequentialExecution = false;
    while (auto token = getNextToken())
    {
        switch (token.value())
        {
            case TokenType::CREATE: {
                const auto firstLine = physicalLine(currentLine);
                auto [query, attachment] = expectCreateStatement();
                parsedTestFile.fixtures.push_back(FixtureStatement{
                    .sql = std::move(query),
                    .attachment = std::move(attachment),
                    .source = Origin{.file = sourceFile, .firstLine = firstLine, .lastLine = physicalLine(currentLine)}});
                break;
            }
            case TokenType::QUERY: {
                const auto firstLine = physicalLine(currentLine);
                auto query = expectQuery(DefaultQueryStopTokens);
                const auto lastLine = physicalLine(currentLine == 0 ? 0 : currentLine - 1);
                expectedResultType = ResultType::TUPLES;
                lastParsedQuery = query;
                auto queryId = queryIdAssigner.getNextQueryNumber();
                lastParsedQueryId = queryId;
                std::optional<CaseKey> runAfter;
                if (sequentialExecution && previousCaseId)
                {
                    runAfter = CaseKey{.relativeTestFile = relativeTestFile, .queryNumber = *previousCaseId};
                }
                parsedTestFile.cases.push_back(ParsedCase{
                    .key = CaseKey{.relativeTestFile = relativeTestFile, .queryNumber = queryId},
                    .source = Origin{.file = sourceFile, .firstLine = firstLine, .lastLine = lastLine},
                    .action = QueryAction{.sql = std::move(query), .kind = QueryKind::Execute},
                    .expectation = RowsExpectation{},
                    .runAfter = std::move(runAfter),
                    .configuration = activeConfiguration()});
                previousCaseId = queryId;
                localConfiguration.clear();
                break;
            }
            case TokenType::EXPLAIN: {
                const auto firstLine = physicalLine(currentLine);
                auto statement = expectQuery(DefaultQueryStopTokens);
                const auto lastLine = physicalLine(currentLine == 0 ? 0 : currentLine - 1);
                expectedResultType = ResultType::VERBATIM;
                lastParsedQuery.reset();
                lastParsedQueryId.reset();
                auto queryId = queryIdAssigner.getNextQueryNumber();
                std::optional<CaseKey> runAfter;
                if (sequentialExecution && previousCaseId)
                {
                    runAfter = CaseKey{.relativeTestFile = relativeTestFile, .queryNumber = *previousCaseId};
                }
                parsedTestFile.cases.push_back(ParsedCase{
                    .key = CaseKey{.relativeTestFile = relativeTestFile, .queryNumber = queryId},
                    .source = Origin{.file = sourceFile, .firstLine = firstLine, .lastLine = lastLine},
                    .action = QueryAction{.sql = std::move(statement), .kind = QueryKind::Explain},
                    .expectation = TextExpectation{},
                    .runAfter = std::move(runAfter),
                    .configuration = {}});
                previousCaseId = queryId;
                localConfiguration.clear();
                break;
            }
            case TokenType::RESULT_DELIMITER: {
                const auto delimiterLine = physicalLine(currentLine);
                const auto optionalToken = peekToken();
                if (optionalToken == TokenType::ERROR_EXPECTATION)
                {
                    expectedResultType = ResultType::TUPLES;
                    ++currentLine;
                    auto expectation = expectError();
                    const auto queryId = queryIdAssigner.getNextQueryResultNumber();
                    auto& parsedCase = findParsedCase(queryId);
                    parsedCase.expectation = std::move(expectation);
                    parsedCase.source.lastLine = physicalLine(currentLine);
                }
                else if (expectedResultType == ResultType::VERBATIM)
                {
                    expectedResultType = ResultType::TUPLES;
                    auto verbatimResultLines = expectVerbatimResultLines();
                    const auto queryId = queryIdAssigner.getNextQueryResultNumber();
                    auto& parsedCase = findParsedCase(queryId);
                    parsedCase.expectation
                        = TextExpectation{.lines = std::move(verbatimResultLines), .matching = TextMatchPolicy::Automatic};
                    parsedCase.source.lastLine
                        = currentLine < sourceLineNumbers.size() ? physicalLine(currentLine) : physicalLine(currentLine - 1);
                }
                else
                {
                    auto tuples = expectTuples(false);
                    const auto tuplesEmpty = tuples.empty();
                    const auto queryId = queryIdAssigner.getNextQueryResultNumber();
                    auto& parsedCase = findParsedCase(queryId);
                    parsedCase.expectation = RowsExpectation{.rows = std::move(tuples), .comparison = ComparisonPolicy::UnorderedTypedRows};
                    parsedCase.source.lastLine = tuplesEmpty ? delimiterLine : physicalLine(currentLine - 1);
                }
                break;
            }
            case TokenType::CONFIGURATION: {
                localConfiguration.push_back(expectConfiguration(false));
                break;
            }
            case TokenType::GLOBAL_CONFIGURATION: {
                globalConfiguration.push_back(expectConfiguration(true));
                break;
            }
            case TokenType::DIFFERENTIAL: {
                INVARIANT(
                    expectedResultType == ResultType::TUPLES,
                    "DIFFERENTIAL block cannot follow an EXPLAIN statement (verbatim result expected)");
                INVARIANT(lastParsedQuery.has_value() && lastParsedQueryId.has_value(), "Differential block without preceding query");

                auto [leftQuery, rightQuery] = expectDifferentialBlock();
                const auto mainQueryId = lastParsedQueryId.value();
                auto differentialQueryId = queryIdAssigner.getNextQueryResultNumber();
                auto& parsedCase = findParsedCase(mainQueryId);
                parsedCase.action = DifferentialAction{.leftSql = leftQuery, .rightSql = rightQuery};
                parsedCase.expectation = DifferentialExpectation{};
                parsedCase.source.lastLine = physicalLine(currentLine == 0 ? 0 : currentLine - 1);

                lastParsedQuery = std::move(rightQuery);
                lastParsedQueryId = differentialQueryId;
                break;
            }
            case TokenType::SEQUENTIAL_EXECUTION: {
                sequentialExecution = not sequentialExecution;
                break;
            }
            case TokenType::ERROR_EXPECTATION:
                throw TestException(
                    "Should never run into the ERROR_EXPECTATION token during systest file parsing, but got line: {}", lines[currentLine]);
        }
    }
    queryIdAssigner.verifyComplete();
    return parsedTestFile;
}

void SystestParser::applySubstitutionRules(std::string& line)
{
    for (const auto& rule : substitutionRules)
    {
        size_t pos = 0;
        const std::string& keyword = rule.keyword;
        while ((pos = line.find(keyword, pos)) != std::string::npos)
        {
            const auto isIdentifierCharacter
                = [](const char character) { return std::isalnum(static_cast<unsigned char>(character)) != 0 || character == '_'; };
            const bool leftBoundary
                = !isIdentifierCharacter(keyword.front()) || pos == 0 || !isIdentifierCharacter(line[pos - 1]);
            const auto endPos = pos + keyword.length();
            const bool rightBoundary
                = !isIdentifierCharacter(keyword.back()) || endPos >= line.size() || !isIdentifierCharacter(line[endPos]);

            if (leftBoundary && rightBoundary)
            {
                std::string substring = line.substr(pos, keyword.length());
                rule.ruleFunction(substring);
                line.replace(pos, keyword.length(), substring);
                pos += substring.length();
            }
            else
            {
                pos += keyword.length();
            }
        }
    }
}

std::optional<TokenType> SystestParser::getTokenIfValid(const std::string& line)
{
    /// Query is a special case as it's identifying token is not space seperated
    if (toLowerCase(line).starts_with(toLowerCase(QueryToken)))
    {
        return TokenType::QUERY;
    }

    std::string potentialToken;
    std::istringstream stream(line);
    stream >> potentialToken;

    /// Lookup in map
    const auto* it = std::ranges::find_if(
        stringToToken, [&potentialToken](const auto& pair) { return toLowerCase(pair.first) == toLowerCase(potentialToken); });
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
    if (currentLine < lines.size() && (toLowerCase(lines[currentLine]) == toLowerCase(ResultDelimiter) || ignoreFirst))
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

std::vector<std::string> SystestParser::expectVerbatimResultLines()
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist: {}", currentLine);
    std::vector<std::string> resultLines;
    /// skip the result line `----`
    if (toLowerCase(lines[currentLine]) == toLowerCase(ResultDelimiter))
    {
        currentLine++;
    }
    while (currentLine < lines.size() && !lines[currentLine].starts_with("==END=="))
    {
        resultLines.push_back(lines[currentLine]);
        currentLine++;
    }
    return resultLines;
}

std::pair<std::string, std::optional<SourceDataSpec>> SystestParser::expectCreateStatement()
{
    std::string createQuery;
    std::optional<SourceDataSpec> testData;

    while (currentLine < lines.size())
    {
        const std::string line = lines[currentLine++];
        if (emptyOrComment(line))
        {
            continue;
        }

        createQuery += line;
        if (createQuery.ends_with(';'))
        {
            break;
        }
        createQuery += '\n';
    }

    while (currentLine < lines.size() && emptyOrComment(lines[currentLine]))
    {
        currentLine++;
    }

    if (currentLine < lines.size() && lines[currentLine].starts_with("ATTACH INLINE"))
    {
        InlineSourceData inlineData;
        currentLine++;
        while (currentLine < lines.size() && !emptyOrComment(lines[currentLine]))
        {
            inlineData.rows.push_back(lines[currentLine]);
            currentLine++;
        }
        testData = std::move(inlineData);
        currentLine--;
    }
    else if (currentLine < lines.size() && lines[currentLine].starts_with("ATTACH FILE"))
    {
        testData = FileSourceData{.file = lines[currentLine].substr(std::strlen("ATTACH FILE") + 1)};
    }
    else
    {
        currentLine--;
    }

    return std::make_pair(createQuery, testData);
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
                const auto trimmedQuerySoFar = trimWhiteSpaces(std::string_view(queryString));
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
                    const auto trimmedQuerySoFar = trimWhiteSpaces(std::string_view(queryString));
                    if (trimmedQuerySoFar.empty())
                    {
                        throw SLTUnexpectedToken("Expected query but got empty query string");
                    }
                    if (trimmedQuerySoFar.back() != ';')
                    {
                        throw InvalidQuerySyntax("Queries must end with a semicolon: \"{}\"", trimmedQuerySoFar);
                    }
                    break;
                }
            }
            else
            {
                const auto trimmedLineView = trimWhiteSpaces(std::string_view(line));
                if (!trimmedLineView.empty() && toLowerCase(trimmedLineView) == "differential")
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
        TokenType::RESULT_DELIMITER,
        TokenType::DIFFERENTIAL,
        TokenType::ERROR_EXPECTATION,
        TokenType::CREATE,
        TokenType::CONFIGURATION,
        TokenType::GLOBAL_CONFIGURATION};

    /// Parse the differential query until the next recognized section
    std::string rightQuery = expectQuery(differentialStopTokens);
    const std::string leftQuery = lastParsedQuery.value();

    return {leftQuery, std::move(rightQuery)};
}

ConfigurationDirective SystestParser::expectConfiguration(const bool global) const
{
    INVARIANT(currentLine < lines.size(), "current line to parse should exist");
    const auto kindLabel = global ? "global configuration" : "configuration";
    auto [key, values] = parseConfigurationLine(lines[currentLine], kindLabel);
    const auto physicalLine = sourceLineNumbers.empty() ? 0 : sourceLineNumbers.at(currentLine);
    return ConfigurationDirective{
        .key = std::move(key),
        .values = std::move(values),
        .global = global,
        .source = Origin{.file = sourceFile, .firstLine = physicalLine, .lastLine = physicalLine}};
}

ErrorExpectation SystestParser::expectError() const
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
    INVARIANT(toLowerCase(token) == toLowerCase(ErrorToken), "Expected ERROR token");

    /// Read the error code
    std::string errorStr;
    if (!(stream >> errorStr))
    {
        throw SLTUnexpectedToken("failed to read error code in: {}", line);
    }

    const std::regex numberRegex("^\\d+$");
    if (std::regex_match(errorStr, numberRegex))
    {
        uint64_t code = 0;
        const auto [end, error] = std::from_chars(errorStr.data(), errorStr.data() + errorStr.size(), code);
        const auto existingCode = error == std::errc{} && end == errorStr.data() + errorStr.size() ? errorCodeExists(code) : std::nullopt;
        if (!existingCode)
        {
            throw SLTUnexpectedToken("invalid error code: {} is not defined in ErrorDefinitions.inc", errorStr);
        }
        expectation.code = *existingCode;
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
