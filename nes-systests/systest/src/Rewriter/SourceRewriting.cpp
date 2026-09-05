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

#include <algorithm>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>

#include <Model/ParsedTestFile.hpp>
#include <Model/RewrittenTest.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/SqlParse.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Returns whether a source of this type reads over a connection instead of from a file.
/// Such a source takes an endpoint instead of a path, and needs a server sending its data while it reads.
bool readsFromSocket(const std::string_view sourceType)
{
    return Sql::sameName(sourceType, Sql::Tcp);
}

/// Returns the options that the test wrote on a physical source, or null when it wrote none.
AntlrSQLParser::NamedConfigExpressionSeqContext* declaredOptions(AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition)
{
    const auto* clause = definition->optionsClause();
    return clause != nullptr ? clause->options : nullptr;
}

/// Replaces a physical source's SET clause with the given options, or adds the clause when the source has none.
/// The grammar allows only one SET clause per physical source, so appending a second one is not an option.
std::string
spliceSetClause(SqlParse& parse, AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition, const std::string_view setClause)
{
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
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

}

SourceRewriter::SourceRewriter(const RewriteTarget& target, const QualifiedNames& names) : target{target}, names{names}
{
}

RewrittenSource SourceRewriter::rewrite(SqlParse& parse, const PhysicalSourceDeclaration& declaration)
{
    const auto sourceNumber = ordinal++;

    /// The file that the source itself opens, which goes into its options.
    std::optional<std::filesystem::path> dataFile;
    /// The file whose bytes feed the source, whether it opens the file or a server sends the content.
    /// A measurement counts what a source reads, and it reads the same bytes either way.
    std::optional<std::filesystem::path> inputFile;
    std::optional<InlineData> inlineData;
    std::optional<ServedData> servedData;
    /// A source that produces its own data (e.g., Generator) takes neither.
    if (declaration.attached != nullptr)
    {
        if (readsFromSocket(declaration.definition->type->getText()))
        {
            /// A server sends the source its data, so no file goes into the options.
            std::visit(
                Overloaded{
                    [&](const InlineRows& inlined) { servedData = ServedData{.content = inlined.rows}; },
                    [&](const AttachedFile& attachedFile)
                    {
                        auto file = target.testDataDir / attachedFile.path;
                        inputFile = file;
                        servedData = ServedData{.content = std::move(file)};
                    }},
                *declaration.attached);
        }
        else
        {
            /// The source opens the attached file itself, or the CSV that the rewriter plans for the inline rows.
            std::visit(
                Overloaded{
                    [&](const InlineRows& inlined)
                    {
                        auto file = target.sourceDataFile(sourceNumber);
                        inlineData = InlineData{.path = file, .rows = inlined.rows};
                        dataFile = std::move(file);
                    },
                    [&](const AttachedFile& attachedFile) { dataFile = target.testDataDir / attachedFile.path; }},
                *declaration.attached);
            inputFile = dataFile;
        }
    }

    const auto setClause = setClauseFor(parse, declaredOptions(declaration.definition), dataFile);
    auto sql = rewriteIdentifiers(spliceSetClause(parse, declaration.definition, setClause), names);
    auto statement = [&]() -> SetupStatement
    {
        if (inlineData.has_value())
        {
            return StatementWithInlineData{.sql = std::move(sql), .data = std::move(*inlineData)};
        }
        if (servedData.has_value())
        {
            return StatementWithServedData{.sql = std::move(sql), .data = std::move(*servedData)};
        }
        return PlainStatement{.sql = std::move(sql)};
    }();
    return RewrittenSource{.statement = std::move(statement), .inputFile = std::move(inputFile)};
}

std::string SourceRewriter::setClauseFor(
    SqlParse& parse, AntlrSQLParser::NamedConfigExpressionSeqContext* declared, const std::optional<std::filesystem::path>& dataFile) const
{
    std::vector<std::string> options;
    if (dataFile.has_value())
    {
        options.push_back(Sql::option(Sql::Source, Sql::FilePath, dataFile->string()));
    }
    if (not declaresOption(declared, Sql::Source, Sql::Host))
    {
        options.push_back(Sql::option(Sql::Source, Sql::Host, target.sourceHost.view()));
    }
    if (not declaresOption(declared, Sql::InputFormatter, Sql::Type))
    {
        options.push_back(Sql::option(Sql::InputFormatter, Sql::Type, Sql::Csv));
    }
    if (const auto declaredText = parse.textOf(declared); not declaredText.empty())
    {
        options.push_back(declaredText);
    }
    return Sql::setClause(Sql::optionList(options));
}

void makeAnonymousSourcePathsAbsolute(
    const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const std::filesystem::path& testDataDir)
{
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parse.tree()))
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

void completeAnonymousSources(const SqlParse& parse, antlr4::TokenStreamRewriter& rewriter, const Host& host)
{
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parse.tree()))
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
    SqlParse parse{sql};
    auto* definition = findFirst<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parse.tree());
    if (definition == nullptr)
    {
        throw TestException("Only a physical source takes source options, but this statement declares something else: {}", sql);
    }

    auto merged = options;
    if (const auto declaredText = parse.textOf(declaredOptions(definition)); not declaredText.empty())
    {
        merged.push_back(declaredText);
    }
    return spliceSetClause(parse, definition, Sql::setClause(Sql::optionList(merged)));
}

}
