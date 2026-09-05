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
#include <Rewriter/SqlParse.hpp>
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
    return not Sql::sameName(sinkType, Sql::Void);
}

/// Returns whether a sink of this type writes a checksum instead of the rows.
/// Such a sink quotes its strings, because the expected checksums were computed over quoted strings.
bool writesChecksum(const std::string_view sinkType)
{
    return Sql::sameName(sinkType, Sql::Checksum);
}

}

AntlrSQLParser::SinkContext* requireSingleSink(const SqlParse& parse, const std::string& sql)
{
    auto* sinkClause = findFirst<AntlrSQLParser::SinkClauseContext>(parse.tree());
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

RewrittenSink SinkRewriter::inlined(
    const std::string& type,
    const std::optional<std::string>& declaredFormat,
    const bool hostAlreadySet,
    const bool quoteStringsAlreadySet,
    const std::string& trailingOptions,
    const std::filesystem::path& candidateResultFile) const
{
    std::vector<std::string> options;
    if (not hostAlreadySet)
    {
        options.push_back(Sql::option(Sql::Sink, Sql::Host, target.sinkHost.view()));
    }
    std::optional<std::filesystem::path> resultFile;
    if (writesResultFile(type))
    {
        resultFile = candidateResultFile;
        options.push_back(Sql::option(Sql::Sink, Sql::FilePath, resultFile->string()));
        if (not declaredFormat.has_value())
        {
            options.push_back(Sql::option(Sql::Sink, Sql::OutputFormat, Sql::Csv));
        }
    }
    if (writesChecksum(type) and not quoteStringsAlreadySet)
    {
        options.push_back(Sql::option(Sql::OutputFormatter, Sql::QuoteStrings, "true"));
    }
    if (not trailingOptions.empty())
    {
        options.push_back(trailingOptions);
    }
    return {.sql = Sql::sink(type, Sql::optionList(options)), .resultFile = std::move(resultFile)};
}

RewrittenSink
SinkRewriter::inlineSink(SqlParse& parse, AntlrSQLParser::SinkContext* sink, const std::filesystem::path& candidateResultFile) const
{
    if (auto* sinkName = sink->identifier(); sinkName != nullptr)
    {
        const auto declaration = sinkByName.find(Identifier::parse(sinkName->getText()));
        if (declaration == sinkByName.end())
        {
            throw TestException("Query writes to sink '{}' that no sink declaration in this file names", sinkName->getText());
        }
        return inlined(
            declaration->second.type,
            std::nullopt,
            false,
            false,
            Sql::schemaOption(Sql::Sink, declaration->second.schema),
            candidateResultFile);
    }
    if (const auto* anonymous = sink->anonymousSink(); anonymous != nullptr)
    {
        if (declaresOption(anonymous->parameters, Sql::Sink, Sql::FilePath))
        {
            throw TestException(
                "A sink written into a query must not choose its result file, because the checker reads the file the rewriter chose: {}",
                sink->getText());
        }
        const auto declaredFormat = declaredOptionValue(anonymous->parameters, Sql::Sink, Sql::OutputFormat);
        const bool hostSet = declaresOption(anonymous->parameters, Sql::Sink, Sql::Host);
        const bool quoteStringsSet = declaresOption(anonymous->parameters, Sql::OutputFormatter, Sql::QuoteStrings);
        return inlined(
            anonymous->type->getText(), declaredFormat, hostSet, quoteStringsSet, parse.textOf(anonymous->parameters), candidateResultFile);
    }
    throw TestException(
        "A query sink that is neither a declared sink nor one written into the query is not supported: {}", sink->getText());
}

std::string SinkRewriter::declaredSinkStatement(SqlParse& parse, const AntlrSQLParser::CreateSinkDefinitionContext* definition) const
{
    const std::vector options{
        Sql::option(Sql::Sink, Sql::Host, target.sinkHost.view()),
        Sql::option(Sql::Sink, Sql::FilePath, target.resultFile(definition->sinkName->getText()).string()),
        Sql::option(Sql::Sink, Sql::OutputFormat, Sql::Csv)};

    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    rewriter.insertAfter(definition->getStop(), fmt::format(" {}", Sql::setClause(Sql::optionList(options))));
    return rewriteIdentifiers(rewriter.getText(), names);
}

}
