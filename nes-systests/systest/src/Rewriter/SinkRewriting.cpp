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
#include <span>
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
    auto* intoTerm = findFirst<AntlrSQLParser::IntoTermContext>(parse.tree());
    if (intoTerm == nullptr or intoTerm->sink().empty())
    {
        throw TestException("A systest query must write into a sink: {}", sql);
    }
    if (intoTerm->sink().size() > 1)
    {
        throw TestException("A query writing into more than one sink is not supported yet: {}", sql);
    }
    return intoTerm->sink().front();
}

bool isNamedSinkReference(AntlrSQLParser::SinkContext* sink)
{
    return sink != nullptr and dynamic_cast<const AntlrSQLParser::FunctionReferenceContext*>(sink->functionCall()) != nullptr;
}

SinkRewriter::SinkRewriter(const RewriteContext& context, const PrefixedNames& names, const SinkByName& sinkByName)
    : context{context}, names{names}, sinkByName{sinkByName}
{
}

/// Builds the anonymous sink that replaces a declared sink or one written into the query.
/// A default is added only for an option that the test did not set, and a declared sink sets none.
RewrittenSink SinkRewriter::inlined(
    const std::string& type,
    const std::span<AntlrSQLParser::NamedFunctionCallArgumentContext* const> declared,
    const std::string& trailingOptions,
    const std::filesystem::path& candidateResultFile) const
{
    /// A sink that writes nothing that a test can read leaves the query with nothing to compare, so the two known ones fail here.
    /// A type that this rewriter does not know passes through, so the engine reports it and a test can expect that error.
    /// VoidSink is the one that a test picks deliberately, to run a query that it does not check.
    if (Sql::sameName(type, Sql::PrintSink) or Sql::sameName(type, Sql::MqttSink))
    {
        throw TestException(
            "A query writes into a sink of type {}, whose result no test can check. Use FileSink, ChecksumSink or VoidSink.", type);
    }

    std::vector<std::string> options;
    if (not declaresOption(declared, Sql::Sink, Sql::Host))
    {
        options.push_back(Sql::option(Sql::Sink, Sql::Host, context.sinkHost.view()));
    }
    const std::optional<std::filesystem::path> resultFile
        = Sql::writesReadableResult(type) ? std::optional{candidateResultFile} : std::nullopt;
    if (resultFile.has_value())
    {
        options.push_back(Sql::option(Sql::sinkFilePathGroup(type), Sql::FilePath, resultFile->string()));
        if (not declaresOption(declared, Sql::OutputFormatter, Sql::Type))
        {
            options.push_back(Sql::option(Sql::OutputFormatter, Sql::Type, Sql::Csv));
        }
    }
    /// A sink that writes a checksum instead of the rows quotes its strings, because the expected checksums were
    /// computed over quoted strings.
    if (Sql::sameName(type, Sql::ChecksumSink) and not declaresOption(declared, Sql::CsvOutputFormatterGroup, Sql::QuoteStrings))
    {
        options.push_back(Sql::literalOption(Sql::CsvOutputFormatterGroup, Sql::QuoteStrings, "true"));
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
    if (auto* ref = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(sink->functionCall()))
    {
        const auto parsedName = Identifier::tryParse(ref->identifier()->getText());
        const auto declaration = parsedName.has_value() ? sinkByName.find(*parsedName) : sinkByName.end();
        if (declaration == sinkByName.end())
        {
            throw TestException("Query writes to sink '{}' that no sink declaration in this file names", ref->identifier()->getText());
        }
        return inlined(declaration->second.type, {}, Sql::schemaOption(Sql::Sink, declaration->second.schema), candidateResultFile);
    }
    if (const auto* emptyCall = dynamic_cast<AntlrSQLParser::EmptyFunctionCallContext*>(sink->functionCall()))
    {
        if (auto* calleeRef = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(emptyCall->callee))
        {
            return inlined(calleeRef->identifier()->getText(), {}, {}, candidateResultFile);
        }
    }
    if (auto* namedCall = dynamic_cast<AntlrSQLParser::NamedFunctionCallContext*>(sink->functionCall()))
    {
        if (auto* calleeRef = dynamic_cast<AntlrSQLParser::FunctionReferenceContext*>(namedCall->callee))
        {
            const auto params = namedCall->namedFunctionCallArgument();
            if (declaresOption(params, Sql::FileSinkGroup, Sql::FilePath)
                or declaresOption(params, Sql::ChecksumSinkGroup, Sql::FilePath)
                or declaresOption(params, Sql::Sink, Sql::FilePath))
            {
                throw TestException(
                    "A sink written into a query must not choose its result file, because the checker reads the file the rewriter chose: "
                    "{}",
                    sink->getText());
            }
            const auto trailingText
                = params.empty() ? std::string{} : parse.tokenStream().getText(params.front()->getStart(), params.back()->getStop());
            return inlined(calleeRef->identifier()->getText(), params, trailingText, candidateResultFile);
        }
    }
    return {.sql = parse.getTextOf(sink), .resultFile = std::nullopt};
}

std::string SinkRewriter::declaredSinkStatement(SqlParse& parse, AntlrSQLParser::CreateSinkDefinitionContext* definition) const
{
    auto* declared = declaredOptions(definition);
    if (declaresOption(declared, Sql::FileSinkGroup, Sql::FilePath)
        or declaresOption(declared, Sql::ChecksumSinkGroup, Sql::FilePath)
        or declaresOption(declared, Sql::Sink, Sql::FilePath))
    {
        throw TestException(
            "A declared sink must not choose its result file, because the checker reads the file the rewriter chose: {}",
            definition->getText());
    }

    std::vector<std::string> options;
    if (not declaresOption(declared, Sql::Sink, Sql::Host))
    {
        options.push_back(Sql::option(Sql::Sink, Sql::Host, context.sinkHost.view()));
    }
    const auto sinkType = definition->type->getText();
    if (Sql::writesReadableResult(sinkType))
    {
        options.push_back(
            Sql::option(Sql::sinkFilePathGroup(sinkType), Sql::FilePath, context.resultFile(definition->sinkName->getText()).string()));
        if (not declaresOption(declared, Sql::OutputFormatter, Sql::Type))
        {
            options.push_back(Sql::option(Sql::OutputFormatter, Sql::Type, Sql::Csv));
        }
    }
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
