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
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <Recognizer.h>
#include <Token.h>
#include <tree/TerminalNode.h>

#include <Rewriter/Constants.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

std::vector<std::string> extractQualifiedParts(const AntlrSQLParser::QualifiedIdentifierContext* qualified)
{
    std::vector<std::string> parts;
    if (qualified == nullptr)
    {
        return parts;
    }
    for (auto* child : qualified->children)
    {
        if (auto* id = dynamic_cast<AntlrSQLParser::IdentifierContext*>(child))
        {
            parts.push_back(id->getText());
        }
        else if (auto* terminal = dynamic_cast<antlr4::tree::TerminalNode*>(child);
                 terminal != nullptr and terminal->getSymbol()->getType() == AntlrSQLLexer::SCHEMA)
        {
            parts.emplace_back(Sql::Schema);
        }
    }
    return parts;
}

AntlrSQLParser::IdentifierContext* extractScalarFunctionReference(AntlrSQLParser::FunctionCallArgumentContext* arg)
{
    auto* scalarArg = dynamic_cast<AntlrSQLParser::ScalarFunctionArgContext*>(arg);
    if (scalarArg == nullptr)
    {
        return nullptr;
    }
    auto* fnScalar = dynamic_cast<AntlrSQLParser::FunctionCallScalarExpressionContext*>(scalarArg->openScalarExpression());
    if (fnScalar == nullptr)
    {
        return nullptr;
    }
    auto* ref = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(fnScalar->functionCall());
    return ref != nullptr ? ref->identifier() : nullptr;
}

}

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

bool isSingleIdentifier(const AntlrSQLParser::QualifiedIdentifierContext* qualified, const std::string_view name)
{
    const auto parts = extractQualifiedParts(qualified);
    return parts.size() == 1 and Sql::sameName(parts.at(0), name);
}

bool isOption(const AntlrSQLParser::QualifiedIdentifierContext* qualified, const std::string_view group, const std::string_view key)
{
    const auto parts = extractQualifiedParts(qualified);
    return parts.size() == 2 and Sql::sameName(parts.at(0), group) and Sql::sameName(parts.at(1), key);
}

bool isOption(AntlrSQLParser::NamedConfigLiteralContext* option, const std::string_view group, const std::string_view key)
{
    return option != nullptr and isOption(option->qualifiedIdentifier(), group, key);
}

bool isOption(const AntlrSQLParser::NamedFunctionCallArgumentContext* option, const std::string_view group, const std::string_view key)
{
    return option != nullptr and isOption(option->name, group, key);
}

bool declaresOption(AntlrSQLParser::NamedConfigLiteralSeqContext* options, const std::string_view group, const std::string_view key)
{
    return options != nullptr
        and std::ranges::any_of(options->namedConfigLiteral(), [&](auto* option) { return isOption(option, group, key); });
}

bool declaresOption(
    const std::span<AntlrSQLParser::NamedFunctionCallArgumentContext* const> options,
    const std::string_view group,
    const std::string_view key)
{
    return std::ranges::any_of(options, [&](auto* option) { return isOption(option, group, key); });
}

AntlrSQLParser::StringLiteralContext* getStringValueOf(AntlrSQLParser::NamedConfigLiteralContext* option)
{
    if (option == nullptr or option->configLiteral() == nullptr or option->configLiteral()->configScalarLiteral() == nullptr)
    {
        return nullptr;
    }
    return option->configLiteral()->configScalarLiteral()->stringLiteral();
}

AntlrSQLParser::StringLiteralContext* getStringValueOf(AntlrSQLParser::FunctionCallArgumentContext* argument)
{
    auto* scalarArg = dynamic_cast<AntlrSQLParser::ScalarFunctionArgContext*>(argument);
    if (scalarArg == nullptr)
    {
        return nullptr;
    }
    auto* literalExpr = dynamic_cast<AntlrSQLParser::LiteralScalarExpressionContext*>(scalarArg->openScalarExpression());
    if (literalExpr == nullptr or literalExpr->literal() == nullptr or literalExpr->literal()->scalarLiteral() == nullptr)
    {
        return nullptr;
    }
    return literalExpr->literal()->scalarLiteral()->stringLiteral();
}

AntlrSQLParser::StringLiteralContext* getStringValueOf(AntlrSQLParser::NamedFunctionCallArgumentContext* option)
{
    return option != nullptr ? getStringValueOf(option->functionCallArgument()) : nullptr;
}

std::string unquote(const std::string& literal)
{
    return replaceAll(literal.substr(1, literal.size() - 2), "''", "'");
}

std::optional<std::string>
declaredOptionValue(AntlrSQLParser::NamedConfigLiteralSeqContext* options, const std::string_view group, const std::string_view key)
{
    if (options == nullptr)
    {
        return std::nullopt;
    }
    for (auto* option : options->namedConfigLiteral())
    {
        if (auto* value = getStringValueOf(option); value != nullptr and isOption(option, group, key))
        {
            return unquote(value->getText());
        }
    }
    return std::nullopt;
}

std::vector<AntlrSQLParser::IdentifierContext*> catalogReferencesInFrom(antlr4::tree::ParseTree* tree)
{
    std::vector<AntlrSQLParser::IdentifierContext*> refs;
    for (auto* closed : findAll<AntlrSQLParser::ClosedRelationalExpressionContext>(tree))
    {
        auto* fnCall = closed->functionCall();
        if (fnCall == nullptr)
        {
            continue;
        }
        if (auto* directRef = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(fnCall))
        {
            refs.push_back(directRef->identifier());
            continue;
        }
        if (auto* posCall = dynamic_cast<AntlrSQLParser::PositionalFunctionCallContext*>(fnCall))
        {
            for (auto* arg : posCall->functionCallArgument())
            {
                if (auto* argRef = extractScalarFunctionReference(arg))
                {
                    refs.push_back(argRef);
                }
            }
        }
        else if (auto* namedCall = dynamic_cast<AntlrSQLParser::NamedFunctionCallContext*>(fnCall))
        {
            for (auto* namedArg : namedCall->namedFunctionCallArgument())
            {
                if (auto* argRef = extractScalarFunctionReference(namedArg->functionCallArgument()))
                {
                    refs.push_back(argRef);
                }
            }
        }
    }
    return refs;
}

std::vector<AnonymousSourceCall> anonymousSourcesInFrom(antlr4::tree::ParseTree* tree)
{
    std::vector<AnonymousSourceCall> sources;
    for (auto* closed : findAll<AntlrSQLParser::ClosedRelationalExpressionContext>(tree))
    {
        auto* fnCall = closed->functionCall();
        if (fnCall == nullptr)
        {
            continue;
        }
        if (auto* emptyCall = dynamic_cast<AntlrSQLParser::EmptyFunctionCallContext*>(fnCall))
        {
            if (auto* calleeRef = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(emptyCall->callee);
                calleeRef != nullptr and not Sql::sameName(calleeRef->identifier()->getText(), Sql::ModelInference))
            {
                sources.push_back(AnonymousSourceCall{
                    .callee = calleeRef->identifier(), .arguments = {}, .closeParen = emptyCall->CLOSE_PARANTHESIS()->getSymbol()});
            }
        }
        else if (auto* namedCall = dynamic_cast<AntlrSQLParser::NamedFunctionCallContext*>(fnCall))
        {
            if (auto* calleeRef = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(namedCall->callee);
                calleeRef != nullptr and not Sql::sameName(calleeRef->identifier()->getText(), Sql::ModelInference))
            {
                sources.push_back(AnonymousSourceCall{
                    .callee = calleeRef->identifier(),
                    .arguments = namedCall->namedFunctionCallArgument(),
                    .closeParen = namedCall->CLOSE_PARANTHESIS()->getSymbol()});
            }
        }
    }
    return sources;
}

}
