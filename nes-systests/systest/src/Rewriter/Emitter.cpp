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
#include <Model/Expectation.hpp>
#include <Model/RunnableTest.hpp>
#include <Model/SystestQueryId.hpp>
#include <Parser/SystestParser.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/ParsedStatement.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

Emitter::Emitter(const RewriteTarget& target, Declarations declarations)
    : target{target}
    , names{std::move(declarations.names)}
    , sinkByName{std::move(declarations.sinkByName)}
    , declaresSinks{declarations.declaresSinks}
    , sourceRewriter{target, names}
    , sinkRewriter{target, names, sinkByName}
{
    runnable.name = target.displayName;
}

RunnableTest Emitter::emit(std::vector<PreparedStatement>& prepared) &&
{
    /// Every CREATE statement runs as setup before any case, so a data file that a test attaches below one of its queries
    /// still feeds that query. Emitting all CREATE statements first makes the attached files known before any query
    /// resolves its input files.
    for (auto& statement : prepared)
    {
        if (auto* create = std::get_if<PreparedCreate>(&statement))
        {
            emitCreate(*create);
        }
    }
    for (auto& statement : prepared)
    {
        std::visit(
            Overloaded{
                [](const PreparedCreate&) {},
                [this](const Systest::QueryStatement* query)
                {
                    auto rewritten = rewriteQuery(query->sql, query->id, query->expected, {});
                    runnable.cases.push_back(RunnableCase{.action = std::move(rewritten), .runsAfterPrevious = query->sequential});
                },
                [this](const Systest::DifferentialStatement* block) { emitDifferential(*block); },
                [this](const Systest::ExplainStatement* explain) { emitExplain(*explain); }},
            statement);
    }
    return std::move(runnable);
}

void Emitter::emitCreate(PreparedCreate& prepared)
{
    std::visit(
        Overloaded{
            [&](const CreateLogicalSourceStatement&)
            {
                /// Rewrite all identifiers in the statement with their qualified spellings
                runnable.createStmts.push_back(
                    RunnableCreateStatement{.sql = rewriteIdentifiers(prepared.create->sql, names), .staged = std::nullopt});
            },
            [&](const CreatePhysicalSourceStatement& declaration)
            {
                auto [statement, inputFile] = sourceRewriter.rewrite(*prepared.parsed, declaration);
                runnable.createStmts.push_back(std::move(statement));
                if (inputFile.has_value())
                {
                    /// Keyed by the qualified name, because a query is parsed after its identifiers were substituted and
                    /// refers to the source by the spelling that the catalog sees.
                    const auto logical = declaration.definition->logicalSource->getText();
                    inputFilesBySource[Identifier::parse(names.qualified(logical).value_or(logical))].push_back(*inputFile);
                }
            },
            [&](const CreateModelStatement& declaration) { runnable.createStmts.push_back(modelStatement(*prepared.parsed, declaration)); },
            [&](const CreateSinkStatement& declaration)
            {
                /// A test file that inlines its sinks emits nothing here, because the declaring phase already held this one.
                /// A test file that contains an EXPLAIN needs the declared sink in the catalog, so a plan that names it
                /// can print it.
                if (declaresSinks)
                {
                    runnable.createStmts.push_back(RunnableCreateStatement{
                        .sql = sinkRewriter.declaredSinkStatement(*prepared.parsed, declaration.definition), .staged = std::nullopt});
                }
            }},
        prepared.classified);
}

RunnableCreateStatement Emitter::modelStatement(ParsedStatement& parsed, const CreateModelStatement& declaration) const
{
    antlr4::TokenStreamRewriter rewriter{&parsed.tokenStream()};
    if (const std::filesystem::path declared{unquote(declaration.definition->modelPath->getText())}; declared.is_relative())
    {
        rewriter.replace(declaration.definition->modelPath, Sql::stringLiteral((target.testDataDir / declared).string()));
    }
    return RunnableCreateStatement{.sql = rewriteIdentifiers(rewriter.getText(), names), .staged = std::nullopt};
}

RunnableQuery
Emitter::rewriteQuery(const std::string& sql, const SystestQueryId id, Expectation expected, const std::string_view resultDiscriminator)
{
    /// Qualifying the source references comes first, because everything below locates and replaces text in the qualified statement.
    /// Substituting names reads tokens rather than a parse tree, so it also works on a statement that does not parse.
    auto qualified = rewriteIdentifiers(sql, names);

    /// A statement that the parser rejects goes to the coordinator unchanged, which reports the syntax error against this one query.
    /// Several tests assert exactly that error, and a statement without a parse tree has nothing to inline into.
    /// Such a query never runs, so it writes no result file.
    std::unique_ptr<ParsedStatement> parse;
    try
    {
        parse = std::make_unique<ParsedStatement>(qualified);
    }
    catch (const Exception&)
    {
        return RunnableQuery{
            .sql = std::move(qualified), .id = id, .resultFile = std::nullopt, .inputFiles = {}, .expectation = std::move(expected)};
    }

    auto& parsed = *parse;
    antlr4::TokenStreamRewriter rewriter{&parsed.tokenStream()};

    makeAnonymousSourcePathsAbsolute(parsed, rewriter, target.testDataDir);
    placeAnonymousSources(parsed, rewriter, target.sourceHost);

    const auto queryNumber = id.getRawValue();
    const auto candidateResultFile = target.workingDir / fmt::format("{}{}_{}.csv", target.testFileKey, resultDiscriminator, queryNumber);

    auto* sink = requireSingleSink(parsed, sql);
    auto [inlinedSink, resultFile, rowsAreJson] = sinkRewriter.inlineSink(parsed, sink, candidateResultFile);
    rewriter.replace(sink->getStart(), sink->getStop(), inlinedSink);

    /// The query name maps its submission and its result back to this test file.
    const auto queryName = fmt::format("{}:{}", target.testFileKey, queryNumber);
    rewriter.insertAfter(sink->getStop(), fmt::format(" {}", Sql::setClause(Sql::option(Sql::Query, Sql::Name, queryName))));

    std::vector<std::filesystem::path> inputFiles;
    for (auto* reference : findAll<AntlrSQLParser::NamedSourceContext>(parsed.tree()))
    {
        if (const auto files = inputFilesBySource.find(Identifier::parse(reference->multipartIdentifier()->getText()));
            files != inputFilesBySource.end())
        {
            std::ranges::copy(files->second, std::back_inserter(inputFiles));
        }
    }

    /// How the rows are written is a property of the inlined sink, so the expectation gets it here rather than from the parser.
    if (auto* rows = std::get_if<ExpectedRows>(&expected))
    {
        rows->rowsAreJson = rowsAreJson;
    }

    return RunnableQuery{
        .sql = rewriter.getText(),
        .id = id,
        .resultFile = std::move(resultFile),
        .inputFiles = std::move(inputFiles),
        .expectation = std::move(expected)};
}

void Emitter::emitExplain(const Systest::ExplainStatement& explain)
{
    auto qualified = rewriteIdentifiers(explain.sql, names);
    ParsedStatement parsed{qualified};

    /// A sink that the test declared keeps its name, because the expected plan prints that name and the emitted
    /// declaration makes it resolve.
    /// A sink that is written into the statement is inlined as it is for a query, and the plan prints it as the operator that the optimizer
    /// has not bound yet.
    if (auto* sink = requireSingleSink(parsed, explain.sql); sink->identifier() == nullptr)
    {
        antlr4::TokenStreamRewriter rewriter{&parsed.tokenStream()};
        const auto candidate = target.workingDir / fmt::format("{}_{}.csv", target.testFileKey, explain.id.getRawValue());
        auto [inlined, resultFile, rowsAreJson] = sinkRewriter.inlineSink(parsed, sink, candidate);
        rewriter.replace(sink->getStart(), sink->getStop(), inlined);
        qualified = rewriter.getText();
    }

    /// An EXPLAIN starts no query, so it writes no result file and takes no query name.
    runnable.cases.push_back(RunnableCase{
    .action = RunnableQuery{
        .sql = std::move(qualified),
        .id = explain.id,
        .resultFile = std::nullopt,
        .inputFiles = {},
        .expectation = ExpectedPlan{.lines = explain.expected, .qualifyingPrefix = target.testFileKey + "_"}},
    .runsAfterPrevious = false});
}

void Emitter::emitDifferential(const Systest::DifferentialStatement& block)
{
    /// The second half needs its own result file name, because the halves share one query number.
    static constexpr auto secondHalf = "_DIFFERENTIAL";

    /// Neither half carries an expectation of its own, so the placeholder passed here is discarded with the rest of
    /// the per-query shell below.
    auto first = rewriteQuery(block.firstSql, block.firstId, ExpectedRows{}, {});
    auto second = rewriteQuery(block.secondSql, block.secondId, ExpectedRows{}, secondHalf);
    if (not first.resultFile.has_value() or not second.resultFile.has_value())
    {
        throw TestException("a differential query has to write a result to compare: {}", block.firstSql);
    }

    /// The block is one case, so the input files of both halves combine.
    auto inputFiles = std::move(first.inputFiles);
    std::ranges::move(second.inputFiles, std::back_inserter(inputFiles));

    runnable.cases.push_back(RunnableCase{
    .action = RunnableDifferential{
        .firstSql = std::move(first.sql),
        .firstId = block.firstId,
        .firstResultFile = std::move(*first.resultFile),
        .secondSql = std::move(second.sql),
        .secondId = block.secondId,
        .secondResultFile = std::move(*second.resultFile),
        .inputFiles = std::move(inputFiles)},
    .runsAfterPrevious = block.sequential});
}

}
