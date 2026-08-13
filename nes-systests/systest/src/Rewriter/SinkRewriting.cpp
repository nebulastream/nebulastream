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
#include <vector>

#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>

#include <Identifiers/Identifier.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/SqlParse.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

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

SinkRewriter::SinkRewriter(const RewriteContext& context, const PrefixedNames& names, const SinkByName& sinkByName)
    : context{context}, names{names}, sinkByName{sinkByName}
{
}

/// Builds the anonymous sink that replaces a declared sink or one written into the query.
/// A default is added only for an option that the test did not set.
RewrittenSink SinkRewriter::inlined(
    const std::string& type,
    AntlrSQLParser::NamedConfigExpressionSeqContext* declared,
    const std::string& trailingOptions,
    const std::filesystem::path& candidateResultFile) const
{
    /// A sink that writes nothing that a test can read leaves the query with nothing to compare, so the two known ones fail here.
    /// A type that this rewriter does not know passes through, so the engine reports it and a test can expect that error.
    /// Void is the one that a test picks deliberately, to run a query that it does not check.
    if (Sql::sameName(type, Sql::Print) or Sql::sameName(type, Sql::Mqtt))
    {
        throw TestException("A query writes into a sink of type {}, whose result no test can check. Use File, Checksum or Void.", type);
    }

    std::vector<std::string> options;
    const std::optional<std::filesystem::path> resultFile
        = Sql::writesReadableResult(type) ? std::optional{candidateResultFile} : std::nullopt;
    if (resultFile.has_value())
    {
        options.push_back(Sql::option(Sql::sinkConfigGroup(type), Sql::FilePath, resultFile->string()));
    }
    /// Keep Void's native buffers when the shared systest formatter default is CSV.
    if (Sql::sameName(type, Sql::Void) and not declaresOption(declared, Sql::OutputFormatter, Sql::Type))
    {
        options.push_back(Sql::option(Sql::OutputFormatter, Sql::Type, "NATIVE"));
    }
    /// A sink that writes a checksum instead of the rows quotes its strings, because the expected checksums were
    /// computed over quoted strings.
    if (Sql::sameName(type, Sql::Checksum)
        and Sql::sameName(declaredOptionValue(declared, Sql::OutputFormatter, Sql::Type).value_or(Sql::Csv), Sql::Csv)
        and not declaresOption(declared, Sql::CsvOutputFormatter, Sql::QuoteStrings))
    {
        options.push_back(Sql::literalOption(Sql::CsvOutputFormatter, Sql::QuoteStrings, "true"));
    }
    if (not trailingOptions.empty())
    {
        options.push_back(trailingOptions);
    }
    return {.sql = Sql::sink(type, Sql::optionList(options)), .resultFile = resultFile};
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
        const auto& definition = declaration->second;
        /// Parse the saved raw sql of the sink definition containing all of its options
        SqlParse declaredParse{definition.sql};
        auto* declared = declaredOptions(findFirst<AntlrSQLParser::CreateSinkDefinitionContext>(declaredParse.tree()));
        if (declaresOption(declared, Sql::sinkConfigGroup(definition.type), Sql::FilePath))
        {
            throw TestException(
                "A declared sink must not choose its result file, because the checker reads the file the rewriter chose: {}",
                sinkName->getText());
        }
        std::vector<std::string> options{Sql::schemaOption(Sql::Sink, definition.schema)};
        if (const auto declaredText = declaredParse.getTextOf(declared); not declaredText.empty())
        {
            options.push_back(declaredText);
        }
        return inlined(definition.type, declared, Sql::optionList(options), candidateResultFile);
    }
    if (const auto* anonymous = sink->anonymousSink(); anonymous != nullptr)
    {
        if (declaresOption(anonymous->parameters, Sql::sinkConfigGroup(anonymous->type->getText()), Sql::FilePath))
        {
            throw TestException(
                "A sink written into a query must not choose its result file, because the checker reads the file the rewriter chose: {}",
                sink->getText());
        }
        return inlined(anonymous->type->getText(), anonymous->parameters, parse.getTextOf(anonymous->parameters), candidateResultFile);
    }
    throw TestException(
        "A query sink that is neither a declared sink nor one written into the query is not supported: {}", sink->getText());
}

std::string SinkRewriter::declaredSinkStatement(SqlParse& parse, AntlrSQLParser::CreateSinkDefinitionContext* definition) const
{
    auto* declared = declaredOptions(definition);
    if (declaresOption(declared, Sql::sinkConfigGroup(definition->type->getText()), Sql::FilePath))
    {
        throw TestException(
            "A declared sink must not choose its result file, because the checker reads the file the rewriter chose: {}",
            definition->getText());
    }

    std::vector<std::string> options;
    options.push_back(
        Sql::option(
            Sql::sinkConfigGroup(definition->type->getText()),
            Sql::FilePath,
            context.resultFile(definition->sinkName->getText()).string()));
    /// The options that the test wrote go last, so its own choices read after the defaults that this added.
    if (const auto declaredText = parse.getTextOf(declared); not declaredText.empty())
    {
        options.push_back(declaredText);
    }

    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    prefixNames(parse, rewriter, names);
    insertSetClause(rewriter, definition, Sql::setClause(Sql::optionList(options)));
    return rewriter.getText();
}

}
