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

#include <Rewriter/Emitter.hpp>

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Rewriter/ClassifiedStatement.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/Declarations.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/RewriteTarget.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlParse.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// Try to convert the source name into an identifier.
/// Returns nullopt if the attempt failed, because the name is not a valid identifier.
std::optional<Identifier> sourceName(const std::string& written)
{
    auto parsed = Identifier::tryParse(written);
    if (not parsed)
    {
        return std::nullopt;
    }
    return std::move(*parsed);
}

}

Emitter::Emitter(const RewriteTarget& target, Declarations declarations)
    : target{target}
    , declarations{std::move(declarations)}
    , sourceRewriter{target, this->declarations.names}
    , sinkRewriter{target, this->declarations.names, this->declarations.sinkByName}
{
    runnable.name = target.displayName;
    runnable.qualifyingPrefix = this->declarations.names.qualifyingPrefix();
}

RunnableTestFile Emitter::emit(ClassifiedTestFile classified) &&
{
    emitSetup(std::move(classified.setup), classified.containsExplain);
    emitCases(std::move(classified.cases));
    return std::move(runnable);
}

void Emitter::emitSetup(std::vector<ClassifiedCreate> setup, const bool submitsDeclaredSinks)
{
    for (auto& create : setup)
    {
        emitCreate(std::move(create), submitsDeclaredSinks);
    }
}

void Emitter::emitCases(std::vector<CaseStatement> cases)
{
    for (auto& testCase : cases)
    {
        std::visit(
            Overloaded{
                [this](SelectStatement&& query) { emitQuery(std::move(query)); },
                [this](const DifferentialStatement& block) { emitDifferential(block); },
                [this](ExplainStatement&& explain) { emitExplain(std::move(explain)); }},
            std::move(testCase));
    }
}

void Emitter::emitCreate(ClassifiedCreate create, const bool submitsDeclaredSinks)
{
    std::visit(
        Overloaded{
            [&](const LogicalSourceDeclaration&)
            {
                /// A logical source needs only its identifiers qualified.
                antlr4::TokenStreamRewriter rewriter{&create.parse->tokenStream()};
                qualifyNames(*create.parse, rewriter, declarations.names);
                runnable.setupStatements.emplace_back(PlainStatement{.sql = rewriter.getText()});
            },
            [&](PhysicalSourceDeclaration&& declaration)
            {
                /// Keyed by the written name, because qualifying is an edit on top of a parse of the test's own text, so
                /// a query's tree refers to the source by the spelling that the test wrote.
                const auto source = sourceName(declaration.definition->logicalSource->getText());
                auto [statement, inputFile] = sourceRewriter.rewrite(*create.parse, std::move(declaration));
                runnable.setupStatements.emplace_back(std::move(statement));
                if (inputFile.has_value() and source.has_value())
                {
                    inputFilesBySource[*source].push_back(*inputFile);
                }
            },
            [&](const ModelDeclaration& declaration) { runnable.setupStatements.emplace_back(modelStatement(*create.parse, declaration)); },
            [&](const SinkDeclaration& declaration)
            {
                /// A test file that inlines its sinks emits nothing here, because the declaring phase already stored this one.
                /// An EXPLAIN needs the declared sink in the catalog, so a plan that refers to it can print its name.
                if (submitsDeclaredSinks)
                {
                    runnable.setupStatements.emplace_back(
                        PlainStatement{.sql = sinkRewriter.declaredSinkStatement(*create.parse, declaration.definition)});
                }
            }},
        std::move(create.declaration));
}

PlainStatement Emitter::modelStatement(SqlParse& parse, const ModelDeclaration& declaration) const
{
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    qualifyNames(parse, rewriter, declarations.names);
    if (const std::filesystem::path declared{unquote(declaration.definition->modelPath->getText())}; declared.is_relative())
    {
        rewriter.replace(declaration.definition->modelPath, Sql::stringLiteral((target.testDataDir / declared).string()));
    }
    return PlainStatement{.sql = rewriter.getText()};
}

Emitter::RewrittenSql Emitter::emitSelect(const std::string& sql, const SystestQueryId id, const std::string_view resultDiscriminator)
{
    std::unique_ptr<SqlParse> parse;
    try
    {
        parse = std::make_unique<SqlParse>(sql);
    }
    catch (const Exception&)
    {
        /// A statement the parser rejects is submitted unchanged, so the syntax error is reported against this one query
        /// rather than the rewrite failing every query of the file, and many test files assert exactly that error.
        /// Return early here, since a statement without a valid parse tree is not worth chasing here.
        return RewrittenSql{.sql = sql, .resultFile = std::nullopt, .inputFiles = {}};
    }

    antlr4::TokenStreamRewriter rewriter{&parse->tokenStream()};

    /// Qualifying goes in first, because an edit below may cover a name it replaced, and the later edit then wins: the text
    /// those edits splice in is already written in terms of the catalog.
    qualifyNames(*parse, rewriter, declarations.names);

    makeAnonymousSourcePathsAbsolute(*parse, rewriter, target.testDataDir);
    completeAnonymousSources(*parse, rewriter, target.sourceHost);

    const auto queryNumber = id.getRawValue();
    const auto suffix
        = resultDiscriminator.empty() ? fmt::format("{}", queryNumber) : fmt::format("{}_{}", resultDiscriminator, queryNumber);

    auto* sink = requireSingleSink(*parse, sql);
    auto [inlinedSink, resultFile] = sinkRewriter.inlineSink(*parse, sink, target.resultFile(suffix));
    rewriter.replace(sink->getStart(), sink->getStop(), inlinedSink);

    std::vector<std::filesystem::path> inputFiles;
    for (auto* namedSource : findAll<AntlrSQLParser::NamedSourceContext>(parse->tree()))
    {
        /// A name that no identifier can hold matches no key, because every key comes from a name this file
        /// declared, so it is skipped rather than reported: the query is still submitted, and rejected when it runs.
        const auto name = sourceName(namedSource->multipartIdentifier()->getText());
        if (not name)
        {
            continue;
        }
        if (const auto files = inputFilesBySource.find(*name); files != inputFilesBySource.end())
        {
            std::ranges::copy(files->second, std::back_inserter(inputFiles));
        }
    }

    return RewrittenSql{.sql = rewriter.getText(), .resultFile = std::move(resultFile), .inputFiles = std::move(inputFiles)};
}

void Emitter::emitQuery(SelectStatement query)
{
    auto [sql, resultFile, inputFiles] = emitSelect(query.sql, query.id, {});
    runnable.cases.push_back(RewrittenCase{
        .action = RewrittenQuery{
            .sql = std::move(sql),
            .id = query.id,
            .resultFile = std::move(resultFile),
            .inputFiles = std::move(inputFiles),
            .expectation = std::move(query.expected)}});
}

void Emitter::emitExplain(ExplainStatement explain)
{
    SqlParse parse{explain.sql};
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    qualifyNames(parse, rewriter, declarations.names);

    /// The explained query binds and optimizes as a regular query does, so its sources need the same completion.
    makeAnonymousSourcePathsAbsolute(parse, rewriter, target.testDataDir);
    completeAnonymousSources(parse, rewriter, target.sourceHost);

    if (auto* sink = requireSingleSink(parse, explain.sql); sink->identifier() == nullptr)
    {
        /// This file will never actually be written to (the query will not run), we just need it to get a valid descriptor.
        const auto candidate = target.resultFile(fmt::format("{}", explain.id.getRawValue()));
        rewriter.replace(sink->getStart(), sink->getStop(), sinkRewriter.inlineSink(parse, sink, candidate).sql);
    }

    runnable.cases.push_back(
        RewrittenCase{.action = RewrittenExplain{.sql = rewriter.getText(), .id = explain.id, .expected = std::move(explain.expected)}});
}

void Emitter::emitDifferential(const DifferentialStatement& block)
{
    /// The second half needs its own result file name, because the halves share a query number.
    static constexpr auto SecondHalf = "DIFFERENTIAL";

    auto [firstSql, firstResultFile, unmeasuredFirstInput] = emitSelect(block.firstSql, block.firstId, {});
    auto [secondSql, secondResultFile, unmeasuredSecondInput] = emitSelect(block.secondSql, block.secondId, SecondHalf);
    if (not firstResultFile.has_value() or not secondResultFile.has_value())
    {
        throw TestException("a differential query has to write a result to compare: {}", block.firstSql);
    }

    runnable.cases.push_back(RewrittenCase{
        .action = RewrittenDifferential{
            .firstSql = std::move(firstSql),
            .firstId = block.firstId,
            .firstResultFile = std::move(*firstResultFile),
            .secondSql = std::move(secondSql),
            .secondId = block.secondId,
            .secondResultFile = std::move(*secondResultFile)}});
}

}
