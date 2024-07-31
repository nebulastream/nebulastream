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

#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Parser/SLTParser.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

void SLTParser::registerSubstitutionRule(const std::string& before, const std::string& after)
{
    substitutionRules.push_back({before, after});
}

/// We do not load the file in a constructor, as we want to be able to handle errors
[[nodiscard]] bool SLTParser::loadFile(const std::string& filePath)
{
    currentLine = 0;
    lines.clear();

    std::ifstream infile(filePath);
    if (!infile.is_open() || infile.bad())
    {
        return false;
    }

    std::string line;
    while (std::getline(infile, line))
    {
        applySubstitutionRules(line);
        lines.push_back(line);
    }
    return true;
}

// TODO: make callback arguments typesafe
using QueryCallback = std::function<void(const std::string&)>;
using QueryResultTuplesCallback = std::function<void(std::vector<std::string>&&)>;
using SourceInFileTuplesCallback = std::function<void(std::vector<std::string>&&, std::vector<std::string>&&)>;
using SourceCallback = std::function<void(std::vector<std::string>&&)>;

void SLTParser::registerOnQueryCallback(QueryCallback callback)
{
    onQueryCallback = callback;
}

void SLTParser::registerOnQueryResultTuplesCallback(QueryResultTuplesCallback callback)
{
    onQueryResultTuplesCallback = callback;
}

void SLTParser::registerOnSourceInFileTuplesCallback(SourceInFileTuplesCallback callback)
{
    onSourceInFileTuplesCallback = callback;
}

void SLTParser::registerOnSourceCallback(SourceCallback callback)
{
    onSourceCallback = callback;
}

/// Here we model the structure of the test file by what we `expect` to see.
/// If we encounter something unexpected, we return false.
[[nodiscard]] bool SLTParser::parse()
{
    // check if file is open
    if (lines.empty())
    {
        NES_ERROR("file is not open");
        return false;
    }

    auto token = nextToken();
    while (token != TokenType::END)
    {
        if (isCSVSource(token))
        {
            auto source = expectSource();
            if (onSourceCallback)
            {
                onSourceCallback(std::move(source));
            }
        }
        else if (isSource(token))
        {
            auto source = expectSource();
            auto tuples = expectTuples(true);
            if (onSourceInFileTuplesCallback)
            {
                onSourceInFileTuplesCallback(std::move(source), std::move(tuples));
            }
        }
        else if (isQuery(token))
        {
            auto query = expectQuery();
            if (onQueryCallback)
            {
                onQueryCallback(query);
            }
            auto token = nextToken();
            if (isResultDelimiter(token))
            {
                auto tuples = expectTuples();
                if (onQueryResultTuplesCallback)
                {
                    onQueryResultTuplesCallback(std::move(tuples));
                }
            }
            else
            {
                NES_ERROR("expected result delimiter after query");
                return false;
            }
        }
        else if (isResultDelimiter(token))
        {
            NES_ERROR("got result delimiter without query");
            return false;
        }
        else if (isInvalid(token))
        {
            NES_ERROR("invalid token");
            return false;
        }
        token = nextToken();
    }
    return true;
}

void SLTParser::applySubstitutionRules(std::string& line)
{
    for (const auto& rule : substitutionRules)
    {
        size_t pos = 0;
        while ((pos = line.find(rule.first, pos)) != std::string::npos)
        {
            line.replace(pos, rule.first.length(), rule.second);
            pos += rule.second.length();
        }
    }
}

[[nodiscard]] SLTParser::TokenType SLTParser::nextToken()
{
    if (++currentLine >= lines.size())
    {
        return TokenType::END;
    }

    while (currentLine < lines.size() && emptyOrComment(lines[currentLine]))
    {
        ++currentLine;
    }

    /// Prevents errors when we reach the end of the file after a comment
    if (currentLine >= lines.size())
    {
        return TokenType::END;
    }

    std::vector<std::string> argumentList = {};
    auto& line = lines[currentLine];
    std::istringstream stream(line);
    std::string token;

    while (stream >> token)
    {
        if (token.compare(0, 11, "Query::from") == 0)
        {
            argumentList.push_back("Query::from");
            argumentList.push_back(token.substr(11));
        }
        else
        {
            argumentList.push_back(token);
        }
    }

    auto it = std::find_if(
        stringToToken.begin(), stringToToken.end(), [&argumentList](const auto& pair) { return pair.first == argumentList[0]; });
    if (it != stringToToken.end())
    {
        return it->second;
    }
    NES_ERROR("Unrecognized parameter {}", argumentList[0]);
    return TokenType::INVALID;
}

[[nodiscard]] std::vector<std::string> SLTParser::expectSource()
{
    std::vector<std::string> argumentList = {};
    auto& line = lines[currentLine];
    std::istringstream stream(line);
    std::string token;
    while (stream >> token)
    {
        argumentList.push_back(token);
    }
    return argumentList;
}

/// TODO: add sanity checks if we actually have tuples here
/// not sure if the parser should know about schemas though
[[nodiscard]] std::vector<std::string> SLTParser::expectTuples(bool ignoreFirst)
{
    std::vector<std::string> result;
    // skip the result line (----) if we are still reading that
    if (currentLine < lines.size() && (lines[currentLine] == "----" || ignoreFirst))
    {
        currentLine++;
    }
    // read the tuples until we encounter a new line
    while (currentLine < lines.size() && !lines[currentLine].empty())
    {
        result.push_back(lines[currentLine]);
        currentLine++;
    }
    return result;
}

[[nodiscard]] std::string SLTParser::expectQuery()
{
    std::string query;
    bool firstLine = true;
    while (currentLine < lines.size() && !emptyOrComment(lines[currentLine]))
    {
        // TODO: do not use explicit token
        if (lines[currentLine] == "----")
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

        currentLine++;
    }
    return query;
}
} /// namespace NES