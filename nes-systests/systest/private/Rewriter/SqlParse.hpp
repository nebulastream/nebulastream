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
#include <ParserRuleContext.h>
#include <Recognizer.h>
#include <Token.h>
#include <TokenStreamRewriter.h>
#include <fmt/format.h>
#include <tree/ParseTree.h>

/// One parse of a statement, and the read-only queries that the rewriting passes run on it.
namespace NES
{

/// Throws on a parse error instead of recovering, so a malformed statement fails at once.
class ThrowingErrorListener final : public antlr4::BaseErrorListener
{
public:
    explicit ThrowingErrorListener(std::string statement) : statement{std::move(statement)} { }

private:
    void
    syntaxError(antlr4::Recognizer*, antlr4::Token*, size_t line, size_t column, const std::string& message, std::exception_ptr) override;

    std::string statement;
};

/// Owns one parse of a statement and keeps its token stream alive, so a rewriter can edit it and render the text again.
/// The lexer puts whitespace on a hidden channel, so rendering preserves every part that the rewriter did not touch.
/// The members point at each other, so a copy or a move would point at the original.
/// Both are deleted, so a parse that has to move is held through a pointer.
class SqlParse
{
public:
    explicit SqlParse(const std::string& statement)
        : listener{statement}, input{statement}, lexer{&input}, tokens{&lexer}, parser{&tokens}, root{parse()}
    {
    }

    SqlParse(const SqlParse&) = delete;
    SqlParse& operator=(const SqlParse&) = delete;
    SqlParse(SqlParse&&) = delete;
    SqlParse& operator=(SqlParse&&) = delete;
    ~SqlParse() = default;

    [[nodiscard]] antlr4::tree::ParseTree* tree() const { return root; }

    [[nodiscard]] antlr4::CommonTokenStream& tokenStream() { return tokens; }

    /// Returns a subtree exactly as the statement wrote it, whitespace and quoting included.
    [[nodiscard]] std::string getTextOf(antlr4::ParserRuleContext* node) { return node != nullptr ? tokens.getText(node) : std::string{}; }

private:
    /// Installs the throwing listener on the lexer and the parser, then parses the whole statement.
    /// The default error strategy reports the first syntax error to that listener, which throws before attempting recovery.
    /// A bail-out strategy would instead raise a parser exception with no message, losing the location and the offending token.
    AntlrSQLParser::SingleStatementContext* parse();

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

/// Returns the options that the test wrote on a definition, or null when it wrote none.
template <typename Definition>
AntlrSQLParser::NamedConfigExpressionSeqContext* declaredOptions(Definition* definition)
{
    const auto* clause = definition->optionsClause();
    return clause != nullptr ? clause->options : nullptr;
}

/// Replaces a definition's SET clause with the given options, or adds the clause when it has none.
/// The grammar allows only one SET clause per definition, so appending a second one is not an option.
template <typename Definition>
void insertSetClause(antlr4::TokenStreamRewriter& rewriter, Definition* definition, const std::string_view setClause)
{
    if (const auto* clause = definition->optionsClause(); clause != nullptr)
    {
        rewriter.replace(clause->getStart(), clause->getStop(), std::string{setClause});
    }
    else
    {
        rewriter.insertAfter(definition->getStop(), fmt::format(" {}", setClause));
    }
}

/// Returns whether a config option has exactly the given group and key.
/// The comparison canonicalizes both names as the binder does, so a quoted and an unquoted spelling of one name match.
/// Comparing the parsed name rather than the statement text also stops a value that reads like the name from matching.
[[nodiscard]] bool isOption(const AntlrSQLParser::NamedConfigExpressionContext* option, std::string_view group, std::string_view key);

/// Returns whether an option list sets the given name, so a default is only added where the test set nothing.
/// A statement without an option list sets nothing.
[[nodiscard]] bool declaresOption(AntlrSQLParser::NamedConfigExpressionSeqContext* options, std::string_view group, std::string_view key);

/// Returns the node holding a config option's value when that value is a string literal, and null for a number or a schema.
/// The node rather than the text, so the caller can replace the value in place.
[[nodiscard]] AntlrSQLParser::StringLiteralContext* getStringValueOf(AntlrSQLParser::NamedConfigExpressionContext* option);

/// Returns the content of a string literal, without the quotes around it.
[[nodiscard]] std::string unquote(const std::string& literal);

/// Returns a config option's value, or nullopt when the list does not set the option or sets it to something other than a string literal.
[[nodiscard]] std::optional<std::string>
declaredOptionValue(AntlrSQLParser::NamedConfigExpressionSeqContext* options, std::string_view group, std::string_view key);

}
