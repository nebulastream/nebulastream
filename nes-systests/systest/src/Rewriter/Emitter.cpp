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
#include <Model/RewrittenTest.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlParse.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

Emitter::Emitter(const RewriteTarget& target, Declarations declarations)
    : target{target}
    , declarations{std::move(declarations)}
    , sourceRewriter{target, this->declarations.names}
    , sinkRewriter{target, this->declarations.names, this->declarations.sinkByName}
{
    runnable.name = target.displayName;
    runnable.qualifyingPrefix = this->declarations.names.qualifyingPrefix();
}

RewrittenTest Emitter::emit(ClassifiedTest& classified) &&
{
    emitSetup(classified.statements, classified.containsExplain);
    emitCases(classified.statements);
    return std::move(runnable);
}

void Emitter::emitSetup(std::vector<ClassifiedStatement>& statements, const bool submitsDeclaredSinks)
{
    for (auto& statement : statements)
    {
        if (auto* create = std::get_if<ClassifiedCreate>(&statement))
        {
            emitCreate(*create, submitsDeclaredSinks);
        }
    }
}

void Emitter::emitCases(std::vector<ClassifiedStatement>& statements)
{
    for (auto& statement : statements)
    {
        std::visit(
            Overloaded{/// The setup pass already emitted every CREATE.
                       [](const ClassifiedCreate&) {},
                       [this](const SelectStatement* query)
                       {
                           auto rewritten = rewriteSql(query->sql, query->id, {});
                           runnable.cases.push_back(RewrittenCase{
                        .action = RewrittenQuery{
                            .sql = std::move(rewritten.sql),
                            .id = query->id,
                            .resultFile = std::move(rewritten.resultFile),
                            .expectation = query->expected},
                        .inputFiles = std::move(rewritten.inputFiles),
                        .runsAfterPrevious = query->sequential});
                       },
                       [this](const DifferentialStatement* block) { emitDifferential(*block); },
                       [this](const ExplainStatement* explain) { emitExplain(*explain); }},
            statement);
    }
}

void Emitter::emitCreate(ClassifiedCreate& create, const bool submitsDeclaredSinks)
{
    std::visit(
        Overloaded{
            [&](const LogicalSourceDeclaration&)
            {
                /// A logical source needs only its identifiers qualified.
                runnable.setupStatements.push_back(PlainStatement{.sql = rewriteIdentifiers(create.create->sql, declarations.names)});
            },
            [&](const PhysicalSourceDeclaration& declaration)
            {
                auto [statement, inputFile] = sourceRewriter.rewrite(*create.parse, declaration);
                runnable.setupStatements.push_back(std::move(statement));
                if (inputFile.has_value())
                {
                    /// Keyed by the qualified name, because a query is parsed after its identifiers were substituted and
                    /// refers to the source by the spelling that the catalog sees.
                    const auto logical = declaration.definition->logicalSource->getText();
                    inputFilesBySource[Identifier::parse(declarations.names.qualified(logical).value_or(logical))].push_back(*inputFile);
                }
            },
            [&](const ModelDeclaration& declaration) { runnable.setupStatements.push_back(modelStatement(*create.parse, declaration)); },
            [&](const SinkDeclaration& declaration)
            {
                /// A test file that inlines its sinks emits nothing here, because the declaring phase already stored this one.
                /// An EXPLAIN needs the declared sink in the catalog, so a plan that refers to it can print its name.
                if (submitsDeclaredSinks)
                {
                    runnable.setupStatements.push_back(
                        PlainStatement{.sql = sinkRewriter.declaredSinkStatement(*create.parse, declaration.definition)});
                }
            }},
        create.declaration);
}

PlainStatement Emitter::modelStatement(SqlParse& parse, const ModelDeclaration& declaration) const
{
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    if (const std::filesystem::path declared{unquote(declaration.definition->modelPath->getText())}; declared.is_relative())
    {
        rewriter.replace(declaration.definition->modelPath, Sql::stringLiteral((target.testDataDir / declared).string()));
    }
    return PlainStatement{.sql = rewriteIdentifiers(rewriter.getText(), declarations.names)};
}

Emitter::RewrittenSql Emitter::rewriteSql(const std::string& sql, const SystestQueryId id, const std::string_view resultDiscriminator)
{
    /// Qualifying the source references comes first, because everything below locates and replaces text in the qualified statement.
    /// Substituting names reads tokens rather than a parse tree, so it also works on a statement that does not parse.
    auto qualified = rewriteIdentifiers(sql, declarations.names);

    /// A statement that the parser rejects goes to the coordinator unchanged, which reports the syntax error against this one query.
    /// Several tests assert exactly that error, and a statement without a parse tree has nothing to inline into.
    /// Such a query never runs, so it writes no result file.
    std::unique_ptr<SqlParse> parse;
    try
    {
        parse = std::make_unique<SqlParse>(qualified);
    }
    catch (const Exception&)
    {
        return RewrittenSql{.sql = std::move(qualified), .resultFile = std::nullopt, .inputFiles = {}};
    }

    antlr4::TokenStreamRewriter rewriter{&parse->tokenStream()};

    makeAnonymousSourcePathsAbsolute(*parse, rewriter, target.testDataDir);
    completeAnonymousSources(*parse, rewriter, target.sourceHost);

    const auto queryNumber = id.getRawValue();
    const auto suffix
        = resultDiscriminator.empty() ? fmt::format("{}", queryNumber) : fmt::format("{}_{}", resultDiscriminator, queryNumber);

    auto* sink = requireSingleSink(*parse, sql);
    auto [inlinedSink, resultFile] = sinkRewriter.inlineSink(*parse, sink, target.resultFile(suffix));
    rewriter.replace(sink->getStart(), sink->getStop(), inlinedSink);

    std::vector<std::filesystem::path> inputFiles;
    for (auto* reference : findAll<AntlrSQLParser::NamedSourceContext>(parse->tree()))
    {
        if (const auto files = inputFilesBySource.find(Identifier::parse(reference->multipartIdentifier()->getText()));
            files != inputFilesBySource.end())
        {
            std::ranges::copy(files->second, std::back_inserter(inputFiles));
        }
    }

    return RewrittenSql{.sql = rewriter.getText(), .resultFile = std::move(resultFile), .inputFiles = std::move(inputFiles)};
}

void Emitter::emitExplain(const ExplainStatement& explain)
{
    auto qualified = rewriteIdentifiers(explain.sql, declarations.names);
    SqlParse parse{qualified};
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};

    /// The explained query binds and optimizes as a regular query does, so its sources need the same completion.
    makeAnonymousSourcePathsAbsolute(parse, rewriter, target.testDataDir);
    completeAnonymousSources(parse, rewriter, target.sourceHost);

    /// A sink that the test declared keeps its name, because the expected plan prints that name and the emitted
    /// declaration makes it resolve.
    /// The rewriter inlines a sink written into the statement as it does for a query, and the plan prints it as the operator that the
    /// optimizer has not bound yet.
    if (auto* sink = requireSingleSink(parse, explain.sql); sink->identifier() == nullptr)
    {
        const auto candidate = target.resultFile(fmt::format("{}", explain.id.getRawValue()));
        rewriter.replace(sink->getStart(), sink->getStop(), sinkRewriter.inlineSink(parse, sink, candidate).sql);
    }

    runnable.cases.push_back(RewrittenCase{
        .action = RewrittenExplain{.sql = rewriter.getText(), .id = explain.id, .expected = explain.expected},
        .inputFiles = {},
        .runsAfterPrevious = false});
}

void Emitter::emitDifferential(const DifferentialStatement& block)
{
    /// The second half needs its own result file name, because the halves share one query number.
    static constexpr auto SecondHalf = "DIFFERENTIAL";

    auto first = rewriteSql(block.firstSql, block.firstId, {});
    auto second = rewriteSql(block.secondSql, block.secondId, SecondHalf);
    if (not first.resultFile.has_value() or not second.resultFile.has_value())
    {
        throw TestException("a differential query has to write a result to compare: {}", block.firstSql);
    }

    /// The block is one case, so the input files of both halves combine.
    auto inputFiles = std::move(first.inputFiles);
    std::ranges::move(second.inputFiles, std::back_inserter(inputFiles));

    runnable.cases.push_back(RewrittenCase{
        .action = RewrittenDifferential{
            .firstSql = std::move(first.sql),
            .firstId = block.firstId,
            .firstResultFile = std::move(*first.resultFile),
            .secondSql = std::move(second.sql),
            .secondId = block.secondId,
            .secondResultFile = std::move(*second.resultFile)},
        .inputFiles = std::move(inputFiles),
        .runsAfterPrevious = block.sequential});
}

}
