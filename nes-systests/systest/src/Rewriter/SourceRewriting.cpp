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
#include <Model/RunnableTestFile.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
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

}

SourceRewriter::SourceRewriter(const RewriteContext& context, const PrefixedNames& names) : context{context}, names{names}
{
}

RewrittenSource SourceRewriter::rewrite(SqlParse& parse, PhysicalSourceDeclaration declaration)
{
    /// A physical source reads the data of its ATTACH, whose path is resolved against the test data directory.
    /// The options that the test wrote pass through as written, so a path written here would stay relative,
    /// and one written alongside an ATTACH would set the option twice.
    if (declaresOption(declaredOptions(declaration.definition), Sql::Source, Sql::FilePath))
    {
        throw TestException(
            "A physical source must not choose its data file, because a source reads the data that its ATTACH names: {}",
            declaration.definition->getText());
    }

    if (declaration.attached.has_value() and readsFromSocket(declaration.definition->type->getText())
        and (declaresOption(declaredOptions(declaration.definition), Sql::Source, Sql::SocketHost)
             or declaresOption(declaredOptions(declaration.definition), Sql::Source, Sql::SocketPort)))
    {
        throw TestException(
            "A source with attached data must not choose its socket endpoint, because a server sends it that data: {}",
            declaration.definition->getText());
    }

    const auto sourceNumber = ordinal++;

    /// The file that the source itself opens, which goes into its options.
    std::optional<std::filesystem::path> dataFile;
    /// The file whose bytes feed the source, whether it opens the file or a server sends the content.
    /// A measurement counts what a source reads, and it reads the same bytes either way.
    std::optional<std::filesystem::path> inputFile;
    std::optional<InlineData> inlineData;
    std::optional<ServedData> servedData;
    /// A source that produces its own data (e.g., Generator) takes neither.
    if (declaration.attached.has_value())
    {
        if (readsFromSocket(declaration.definition->type->getText()))
        {
            /// A server sends the source its data, so no file goes into the options.
            std::visit(
                Overloaded{
                    [&](InlineRows&& inlined) { servedData = ServedData{.content = std::move(inlined.rows)}; },
                    [&](const AttachedFile& attachedFile)
                    {
                        auto file = context.testDataDir / attachedFile.path;
                        inputFile = file;
                        servedData = ServedData{.content = std::move(file)};
                    }},
                std::move(*declaration.attached));
        }
        else
        {
            /// The source opens the attached file itself, or the CSV that the rewriter plans for the inline rows.
            std::visit(
                Overloaded{
                    [&](InlineRows&& inlined)
                    {
                        auto file = context.sourceDataFile(sourceNumber);
                        inlineData = InlineData{.path = file, .rows = std::move(inlined.rows)};
                        dataFile = std::move(file);
                    },
                    [&](const AttachedFile& attachedFile) { dataFile = context.testDataDir / attachedFile.path; }},
                std::move(*declaration.attached));
            inputFile = dataFile;
        }
    }

    const auto setClause = setClauseFor(parse, declaredOptions(declaration.definition), dataFile);
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    /// Prefixing goes in first, because the set clause replaces whatever the test declared, and a later edit wins over an
    /// earlier one that it covers.
    prefixNames(parse, rewriter, names);
    insertSetClause(rewriter, declaration.definition, setClause);
    auto sql = rewriter.getText();
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

/// The host pins the source to one worker, because an omitted host resolves to the worker that answers,
/// and a run placed on a topology answers none.
/// The CSV default exists because creating the source's descriptor rejects a source without an input format.
/// The options that the test wrote come last, unchanged.
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
        options.push_back(Sql::option(Sql::Source, Sql::Host, context.sourceHost.view()));
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

std::string addSourceOptions(const std::string& sql, const std::vector<SourceOption>& options)
{
    SqlParse parse{sql};
    auto* definition = findFirst<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parse.tree());
    if (definition == nullptr)
    {
        throw TestException("Only a physical source takes source options, but this statement declares something else: {}", sql);
    }

    auto* declared = declaredOptions(definition);
    std::vector<std::string> merged;
    for (const auto& [group, key, value] : options)
    {
        if (not declaresOption(declared, group, key))
        {
            merged.push_back(Sql::option(group, key, value));
        }
    }
    if (const auto declaredText = parse.textOf(declared); not declaredText.empty())
    {
        merged.push_back(declaredText);
    }
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    insertSetClause(rewriter, definition, Sql::setClause(Sql::optionList(merged)));
    return rewriter.getText();
}

}
