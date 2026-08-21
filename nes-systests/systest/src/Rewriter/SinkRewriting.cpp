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

#include <Rewriter/SinkRewriting.hpp>

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>
#include <fmt/format.h>

#include <Identifiers/Identifier.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/ParsedStatement.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Returns whether a sink of this type writes a result file.
/// The File and Checksum sinks do, and they take a path and an output format.
/// The sink that discards its input takes neither and leaves the query with nothing to check.
bool writesResultFile(const std::string_view sinkType)
{
    return Identifier::parse(std::string{sinkType}) != Identifier::parse(std::string{Sql::Void});
}

}

/// Returns the sink that a query writes into, and rejects a query that writes into none or more than one.
/// A query needs exactly one, because the runner reads back a single result file per query.
AntlrSQLParser::SinkContext* requireSingleSink(const ParsedStatement& parsed, const std::string& sql)
{
    auto* sinkClause = findFirst<AntlrSQLParser::SinkClauseContext>(parsed.tree());
    if (sinkClause == nullptr or sinkClause->sink().empty())
    {
        throw TestException("A systest query must write into a sink: {}", sql);
    }
    if (sinkClause->sink().size() > 1)
    {
        throw TestException("A query writing into more than one sink is not supported yet: {}", sql);
    }
    return sinkClause->sink().front();
}

SinkRewriter::SinkRewriter(const RewriteTarget& target, const QualifiedNames& names, const SinkByName& sinkByName)
    : target{target}, names{names}, sinkByName{sinkByName}
{
}

InlinedSink SinkRewriter::inlineDeclaredSink(const SinkDefinition& declaration, const std::filesystem::path& candidateResultFile) const
{
    std::vector options{Sql::option(Sql::Sink, Sql::Host, target.sinkHost.view())};
    std::optional<std::filesystem::path> resultFile;
    if (writesResultFile(declaration.type))
    {
        resultFile = candidateResultFile;
        options.push_back(Sql::option(Sql::Sink, Sql::FilePath, resultFile->string()));
        options.push_back(Sql::option(Sql::Sink, Sql::OutputFormat, Sql::Csv));
    }
    options.push_back(Sql::schemaOption(Sql::Sink, declaration.schema));
    /// A declared sink writes CSV, which is the format that the rewriter chooses for it.
    return {.sql = Sql::sink(declaration.type, Sql::optionList(options)), .resultFile = std::move(resultFile), .rowsAreJson = false};
}

InlinedSink SinkRewriter::inlineQuerySink(
    ParsedStatement& parsed, const AntlrSQLParser::AnonymousSinkContext* anonymous, const std::filesystem::path& candidateResultFile) const
{
    const auto sinkType = anonymous->type->getText();
    std::vector options{Sql::option(Sql::Sink, Sql::Host, target.sinkHost.view())};
    std::optional<std::filesystem::path> resultFile;
    const auto declaredFormat = declaredOptionValue(anonymous->parameters, Sql::Sink, Sql::OutputFormat);
    if (writesResultFile(sinkType))
    {
        resultFile = candidateResultFile;
        options.push_back(Sql::option(Sql::Sink, Sql::FilePath, resultFile->string()));
        if (not declaredFormat.has_value())
        {
            options.push_back(Sql::option(Sql::Sink, Sql::OutputFormat, Sql::Csv));
        }
    }
    if (const auto declaredText = parsed.textOf(anonymous->parameters); not declaredText.empty())
    {
        options.push_back(declaredText);
    }
    return {
        .sql = Sql::sink(sinkType, Sql::optionList(options)),
        .resultFile = std::move(resultFile),
        .rowsAreJson = declaredFormat.has_value() and Identifier::parse(*declaredFormat) == Identifier::parse(std::string{Sql::Json})};
}

/// Both sink forms become an anonymous sink of the declared type.
/// A sink that writes a result file takes this query's own file, so two queries never write the same one.
InlinedSink
SinkRewriter::inlineSink(ParsedStatement& parsed, AntlrSQLParser::SinkContext* sink, const std::filesystem::path& candidateResultFile) const
{
    if (auto* sinkName = sink->identifier(); sinkName != nullptr)
    {
        const auto declaration = sinkByName.find(Identifier::parse(sinkName->getText()));
        if (declaration == sinkByName.end())
        {
            throw TestException("Query writes to sink '{}' that no sink declaration in this file names", sinkName->getText());
        }
        return inlineDeclaredSink(declaration->second, candidateResultFile);
    }
    if (const auto* anonymous = sink->anonymousSink(); anonymous != nullptr)
    {
        return inlineQuerySink(parsed, anonymous, candidateResultFile);
    }
    throw TestException(
        "A query sink that is neither a declared sink nor one written into the query is not supported: {}", sink->getText());
}

std::string
SinkRewriter::declaredSinkStatement(ParsedStatement& parsed, const AntlrSQLParser::CreateSinkDefinitionContext* definition) const
{
    const auto name = definition->sinkName->getText();
    const std::vector options{
        Sql::option(Sql::Sink, Sql::Host, target.sinkHost.view()),
        Sql::option(Sql::Sink, Sql::FilePath, (target.workingDir / fmt::format("{}_{}.csv", target.testFileKey, name)).string()),
        Sql::option(Sql::Sink, Sql::OutputFormat, Sql::Csv)};

    antlr4::TokenStreamRewriter rewriter{&parsed.tokenStream()};
    rewriter.insertAfter(definition->getStop(), fmt::format(" {}", Sql::setClause(Sql::optionList(options))));
    return rewriteIdentifiers(rewriter.getText(), names);
}

}
