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

#pragma once

#include <algorithm>
#include <cstddef>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <BaseErrorListener.h>
#include <CommonTokenStream.h>
#include <Recognizer.h>
#include <tree/ParseTree.h>

#include <Rewriter/Constants.hpp>
#include <ErrorHandling.hpp>

/// One SQL parse of a statement, and the queries the rewriter runs against it.
/// Everything here reads the statement that the test file wrote.
/// The rewriter decides what to emit in its place.
namespace NES
{

/// Throws on a parse error instead of recovering, so a malformed statement fails at once.
class ThrowingErrorListener final : public antlr4::BaseErrorListener
{
public:
    explicit ThrowingErrorListener(std::string statement) : statement{std::move(statement)} { }

private:
    void
    syntaxError(antlr4::Recognizer*, antlr4::Token*, const size_t line, const size_t column, const std::string& message, std::exception_ptr)
        override
    {
        throw TestException("Could not parse a statement at {}:{}: {} in {}", line, column, message, statement);
    }

    std::string statement;
};

/// Owns one parse of a statement and keeps its token stream alive, so a rewriter can edit it and render the text again.
/// The lexer puts whitespace on a hidden channel, so rendering preserves every part that the rewriter did not touch.
class SqlParse
{
public:
    explicit SqlParse(const std::string& statement)
        : listener{statement}, input{statement}, lexer{&input}, tokens{&lexer}, parser{&tokens}, root{parse()}
    {
    }

    [[nodiscard]] antlr4::tree::ParseTree* tree() const { return root; }

    [[nodiscard]] antlr4::CommonTokenStream& tokenStream() { return tokens; }

    /// Returns a subtree exactly as the statement wrote it, whitespace and quoting included.
    [[nodiscard]] std::string textOf(antlr4::ParserRuleContext* node) { return node != nullptr ? tokens.getText(node) : std::string{}; }

private:
    /// Installs the throwing listener on the lexer and the parser, then parses the whole statement.
    /// The default error strategy reports the first syntax error to that listener, which throws before attempting recovery.
    /// A bail-out strategy would instead raise a parser exception with no message, losing the location and the offending token.
    AntlrSQLParser::SingleStatementContext* parse()
    {
        lexer.removeErrorListeners();
        parser.removeErrorListeners();
        lexer.addErrorListener(&listener);
        parser.addErrorListener(&listener);
        return parser.singleStatement();
    }

    ThrowingErrorListener listener;
    antlr4::ANTLRInputStream input;
    AntlrSQLLexer lexer;
    antlr4::CommonTokenStream tokens;
    AntlrSQLParser parser;
    AntlrSQLParser::SingleStatementContext* root;
};

/// Returns the first node of the given rule type anywhere under the tree, or null when the statement has none.
template <typename Context>
Context* findFirst(antlr4::tree::ParseTree* node)
{
    if (node == nullptr)
    {
        return nullptr;
    }
    if (auto* typed = dynamic_cast<Context*>(node))
    {
        return typed;
    }
    for (auto* child : node->children)
    {
        if (auto* found = findFirst<Context>(child))
        {
            return found;
        }
    }
    return nullptr;
}

namespace detail
{
template <typename Context>
void findAll(antlr4::tree::ParseTree* node, std::vector<Context*>& found)
{
    if (node == nullptr)
    {
        return;
    }
    if (auto* typed = dynamic_cast<Context*>(node))
    {
        found.push_back(typed);
    }
    for (auto* child : node->children)
    {
        findAll<Context>(child, found);
    }
}
}

/// Returns every node of the given rule type anywhere under the tree, in the order they occur in the statement.
template <typename Context>
std::vector<Context*> findAll(antlr4::tree::ParseTree* node)
{
    std::vector<Context*> found;
    detail::findAll<Context>(node, found);
    return found;
}

/// Returns whether a config option has exactly the given qualified name.
/// The comparison canonicalizes both names the way the binder does, so a quoted and an unquoted spelling of one name match.
/// Comparing the parsed name rather than the statement text also stops a value that reads like the name from matching.
inline bool namesOption(AntlrSQLParser::NamedConfigExpressionContext* option, const std::string_view group, const std::string_view key)
{
    const auto& parts = option->name->strictIdentifier();
    return parts.size() == 2 and Sql::sameName(parts.at(0)->getText(), group) and Sql::sameName(parts.at(1)->getText(), key);
}

/// Returns whether an option list sets the given name.
/// The rewriter keeps a value that the test chose instead of writing its own default.
/// A statement without an option list sets nothing.
inline bool
declaresOption(AntlrSQLParser::NamedConfigExpressionSeqContext* options, const std::string_view group, const std::string_view key)
{
    return options != nullptr
        and std::ranges::any_of(options->namedConfigExpression(), [&](auto* option) { return namesOption(option, group, key); });
}

/// Returns the node holding a config option's value when that value is a string literal, and null for a number or a schema.
/// The node rather than the text, so the caller can replace the value in place.
inline AntlrSQLParser::StringLiteralContext* stringValueOf(AntlrSQLParser::NamedConfigExpressionContext* option)
{
    return dynamic_cast<AntlrSQLParser::StringLiteralContext*>(option->constant());
}

/// Returns the content of a string literal, without the quotes around it.
inline std::string unquote(const std::string& literal)
{
    return literal.substr(1, literal.size() - 2);
}

/// Returns a config option's value as a string, or nullopt when the list does not set the option or sets it to something
/// other than a string literal.
/// The caller needs the value that the test chose, not only whether it chose one.
inline std::optional<std::string>
declaredOptionValue(AntlrSQLParser::NamedConfigExpressionSeqContext* options, const std::string_view group, const std::string_view key)
{
    if (options == nullptr)
    {
        return std::nullopt;
    }
    for (auto* option : options->namedConfigExpression())
    {
        if (auto* value = stringValueOf(option); value != nullptr and namesOption(option, group, key))
        {
            return unquote(value->getText());
        }
    }
    return std::nullopt;
}

}
