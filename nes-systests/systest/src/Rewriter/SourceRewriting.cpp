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

#include <Rewriter/SourceRewriting.hpp>

#include <cstddef>
#include <filesystem>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>
#include <Identifiers/Identifier.hpp>
#include <Model/RewrittenTest.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/ParsedStatement.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// The file that a physical source with attached data reads.
/// A test that declared its rows inline instead of pointing at a file also gets the CSV to write there.
/// The ordinal keeps the generated file name unique within the test file.
struct ResolvedAttachment
{
    std::filesystem::path file;
    std::optional<InlineData> inlineData;
};

ResolvedAttachment attachedFileFor(const AttachedData& attach, const RewriteTarget& target, const size_t ordinal)
{
    return std::visit(
        Overloaded{
            [&](const InlineRows& inlined) -> ResolvedAttachment
            {
                const auto file = target.workingDir / "sources" / fmt::format("{}_{}.csv", target.testFileKey, ordinal);
                return {.file = file, .inlineData = InlineData{.path = file, .rows = inlined.rows}};
            },
            [&](const AttachedFile& attachedFile) -> ResolvedAttachment
            { return {.file = target.testDataDir / attachedFile.path, .inlineData = std::nullopt}; }},
        attach);
}

/// Returns whether a source of this type reads over a connection instead of from a file.
/// Such a source takes an endpoint instead of a path, and needs a server sending its data while it reads.
bool readsFromSocket(const std::string_view sourceType)
{
    return Identifier::parse(std::string{sourceType}) == Identifier::parse(std::string{Sql::Tcp});
}

/// Returns the options that the test wrote on a physical source, or null when it wrote none.
AntlrSQLParser::NamedConfigExpressionSeqContext* declaredOptions(AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition)
{
    const auto* clause = definition->optionsClause();
    return clause != nullptr ? clause->options : nullptr;
}

/// Replaces a physical source's SET clause with the given options, or adds the clause when the source has none.
/// The grammar allows only one SET clause per physical source, so appending a second one is not an option.
std::string spliceSetClause(
    ParsedStatement& parsed, AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition, const std::string_view setClause)
{
    antlr4::TokenStreamRewriter rewriter{&parsed.tokenStream()};
    if (const auto* clause = definition->optionsClause(); clause != nullptr)
    {
        rewriter.replace(clause->getStart(), clause->getStop(), std::string{setClause});
    }
    else
    {
        rewriter.insertAfter(definition->getStop(), fmt::format(" {}", setClause));
    }
    return rewriter.getText();
}

/// The file that is served to a source that reads from a socket, when the test pointed at one rather than declaring its rows inline.
/// A measurement counts what a source reads, and it reads the same bytes whether they arrive over a socket or not.
std::optional<std::filesystem::path> servedFile(const Systest::CreatePhysicalSourceStatement& declaration, const RewriteTarget& target)
{
    const auto* attachedFile = declaration.attached.has_value() ? std::get_if<AttachedFile>(&*declaration.attached) : nullptr;
    if (attachedFile == nullptr or not readsFromSocket(declaration.definition->type->getText()))
    {
        return std::nullopt;
    }
    return target.testDataDir / attachedFile->path;
}

}

SourceRewriter::SourceRewriter(const RewriteTarget& target, const QualifiedNames& names) : target{target}, names{names}
{
}

RewrittenSource SourceRewriter::rewrite(ParsedStatement& parsed, const Systest::CreatePhysicalSourceStatement& declaration)
{
    const auto sourceNumber = ordinal++;

    std::optional<std::filesystem::path> dataFile;
    std::optional<StagedData> staged;
    /// A source that produces its own data (e.g., Generator) takes neither.
    if (declaration.attached.has_value())
    {
        /// Server sends the TCP source its data, `dataFile` is not populated.
        if (readsFromSocket(declaration.definition->type->getText()))
        {
            /// Data that a server sends for a source: the test's declared rows, or the file that it pointed at.
            staged = std::visit(
                Overloaded{
                    [](const InlineRows& inlined) { return ServedData{.content = inlined.rows}; },
                    [&](const AttachedFile& attachedFile) { return ServedData{.content = target.testDataDir / attachedFile.path}; }},
                declaration.attached.value());
        }
        else
        {
            /// Source that reads a file takes the path of its attached data.
            auto [file, inlineData] = attachedFileFor(*declaration.attached, target, sourceNumber);
            dataFile = std::move(file);
            if (inlineData.has_value())
            {
                staged = std::move(*inlineData);
            }
        }
    }

    auto inputFile = dataFile.has_value() ? dataFile : servedFile(declaration, target);
    const auto setClause = setClauseFor(parsed, declaredOptions(declaration.definition), dataFile);
    return RewrittenSource{
        .statement
        = RewrittenCreateStatement{.sql = rewriteIdentifiers(spliceSetClause(parsed, declaration.definition, setClause), names), .staged = std::move(staged)},
        .inputFile = std::move(inputFile)};
}

std::string SourceRewriter::setClauseFor(
    ParsedStatement& parsed,
    AntlrSQLParser::NamedConfigExpressionSeqContext* declared,
    const std::optional<std::filesystem::path>& dataFile) const
{
    std::vector<std::string> options;
    if (dataFile.has_value())
    {
        options.push_back(Sql::option(Sql::Source, Sql::FilePath, dataFile->string()));
    }
    options.push_back(Sql::option(Sql::Source, Sql::Host, target.sourceHost.view()));
    if (not declaresOption(declared, Sql::InputFormatter, Sql::Type))
    {
        options.push_back(Sql::option(Sql::InputFormatter, Sql::Type, Sql::Csv));
    }
    if (const auto declaredText = parsed.textOf(declared); not declaredText.empty())
    {
        options.push_back(declaredText);
    }
    return Sql::setClause(Sql::optionList(options));
}

void makeAnonymousSourcePathsAbsolute(
    const ParsedStatement& parsed, antlr4::TokenStreamRewriter& rewriter, const std::filesystem::path& testDataDir)
{
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parsed.tree()))
    {
        for (auto* option : source->parameters->namedConfigExpression())
        {
            if (auto* value = stringValueOf(option); value != nullptr and namesOption(option, Sql::Source, Sql::FilePath))
            {
                if (const std::filesystem::path declared{unquote(value->getText())}; declared.is_relative())
                {
                    rewriter.replace(value->getStart(), value->getStop(), Sql::stringLiteral((testDataDir / declared).string()));
                }
            }
        }
    }
}

void completeAnonymousSources(const ParsedStatement& parsed, antlr4::TokenStreamRewriter& rewriter, const Host& host)
{
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parsed.tree()))
    {
        const auto declared = source->parameters->namedConfigExpression();
        std::vector<std::string> missing;
        if (not std::ranges::any_of(declared, [](auto* option) { return namesOption(option, Sql::Source, Sql::Host); }))
        {
            missing.push_back(Sql::option(Sql::Source, Sql::Host, host.view()));
        }
        if (not std::ranges::any_of(declared, [](auto* option) { return namesOption(option, Sql::InputFormatter, Sql::Type); }))
        {
            missing.push_back(Sql::option(Sql::InputFormatter, Sql::Type, Sql::Csv));
        }
        if (not missing.empty())
        {
            rewriter.insertAfter(source->parameters->getStop(), fmt::format(", {}", Sql::optionList(missing)));
        }
    }
}

std::string addSourceOptions(const std::string& sql, const std::vector<std::string>& options)
{
    ParsedStatement parsed{sql};
    auto* definition = findFirst<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parsed.tree());
    if (definition == nullptr)
    {
        throw TestException("Only a physical source takes source options, but this statement declares something else: {}", sql);
    }

    auto merged = options;
    if (const auto declaredText = parsed.textOf(declaredOptions(definition)); not declaredText.empty())
    {
        merged.push_back(declaredText);
    }
    return spliceSetClause(parsed, definition, Sql::setClause(Sql::optionList(merged)));
}

}
