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

#include <Rewriter/SqlParse.hpp>

#include <algorithm>
#include <cstddef>
#include <exception>
#include <optional>
#include <string>
#include <string_view>

#include <AntlrSQLParser.h>
#include <Recognizer.h>
#include <Token.h>

#include <Rewriter/Constants.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void ThrowingErrorListener::syntaxError(
    antlr4::Recognizer*, antlr4::Token*, const size_t line, const size_t column, const std::string& message, std::exception_ptr)
{
    throw TestException("Could not parse a statement at {}:{}: {} in {}", line, column, message, statement);
}

AntlrSQLParser::SingleStatementContext* SqlParse::parse()
{
    lexer.removeErrorListeners();
    parser.removeErrorListeners();
    lexer.addErrorListener(&listener);
    parser.addErrorListener(&listener);
    return parser.singleStatement();
}

bool isOption(const AntlrSQLParser::NamedConfigExpressionContext* option, const std::string_view group, const std::string_view key)
{
    const auto& parts = option->name->strictIdentifier();
    return parts.size() == 2 and Sql::sameName(parts.at(0)->getText(), group) and Sql::sameName(parts.at(1)->getText(), key);
}

bool declaresOption(AntlrSQLParser::NamedConfigExpressionSeqContext* options, const std::string_view group, const std::string_view key)
{
    return options != nullptr
        and std::ranges::any_of(options->namedConfigExpression(), [&](auto* option) { return isOption(option, group, key); });
}

AntlrSQLParser::StringLiteralContext* getStringValueOf(AntlrSQLParser::NamedConfigExpressionContext* option)
{
    return dynamic_cast<AntlrSQLParser::StringLiteralContext*>(option->constant());
}

std::string unquote(const std::string& literal)
{
    return literal.substr(1, literal.size() - 2);
}

std::optional<std::string>
declaredOptionValue(AntlrSQLParser::NamedConfigExpressionSeqContext* options, const std::string_view group, const std::string_view key)
{
    if (options == nullptr)
    {
        return std::nullopt;
    }
    for (auto* option : options->namedConfigExpression())
    {
        if (auto* value = getStringValueOf(option); value != nullptr and isOption(option, group, key))
        {
            return unquote(value->getText());
        }
    }
    return std::nullopt;
}

}
