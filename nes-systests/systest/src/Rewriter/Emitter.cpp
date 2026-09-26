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
#include <cstddef>
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
#include <Rewriter/NamePrefixer.hpp>
#include <Rewriter/RewriteContext.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlParse.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

/// Distinguishes the result files of a query's sinks.
/// The first sink keeps the query's own suffix, so a query with a single sink writes the file it always did.
std::string sinkSuffix(const std::string_view querySuffix, const size_t sinkIndex)
{
    return sinkIndex == 0 ? std::string{querySuffix} : fmt::format("{}_sink{}", querySuffix, sinkIndex);
}

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

Emitter::Emitter(const RewriteContext& context, Declarations declarations)
    : context{context}
    , declarations{std::move(declarations)}
    , sourceRewriter{context, this->declarations.names}
    , sinkRewriter{context, this->declarations.names, this->declarations.sinkByName}
{
    runnable.name = context.name;
    runnable.originalNames = this->declarations.names.originalNames();
}

RunnableTestFile Emitter::emit(ClassifiedTestFile classified) &&
{
    emitSetup(std::move(classified.setup), classified.containsExplain);
    emitTestCases(std::move(classified.testCases));
    return std::move(runnable);
}

void Emitter::emitSetup(std::vector<ClassifiedCreate> setup, const bool submitsDeclaredSinks)
{
    for (auto& create : setup)
    {
        emitCreate(std::move(create), submitsDeclaredSinks);
    }
}

void Emitter::emitTestCases(std::vector<TestCaseStatement> testCases)
{
    for (auto& testCase : testCases)
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
                /// A logical source needs only its identifiers prefixed.
                antlr4::TokenStreamRewriter rewriter{&create.parse->tokenStream()};
                prefixNames(*create.parse, rewriter, declarations.names);
                runnable.setupStatements.emplace_back(PlainStatement{.sql = rewriter.getText()});
            },
            [&](PhysicalSourceDeclaration&& declaration)
            {
                /// Keyed by the written name, because a query's parse tree refers to the source by the spelling that the test wrote.
                /// Every CREATE is emitted before any test case,
                /// so a query finds its input files even when the source is declared below it.
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
                /// An inlined sink needs no statement of its own.
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
    prefixNames(parse, rewriter, declarations.names);
    /// The worker resolves a relative path against its own working directory, not the test data directory.
    if (const std::filesystem::path declared{unquote(declaration.definition->modelPath->getText())}; declared.is_relative())
    {
        rewriter.replace(declaration.definition->modelPath, Sql::stringLiteral((context.testDataDir / declared).string()));
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
        /// A statement that the parser rejects is submitted unchanged,
        /// so the syntax error is reported against this one query rather than the rewrite failing every query of the file.
        /// Many test files assert exactly that error.
        return RewrittenSql{.sql = sql, .resultFiles = {}, .inputFiles = {}};
    }

    antlr4::TokenStreamRewriter rewriter{&parse->tokenStream()};

    /// Prefixing goes in first, because a later edit wins over an earlier one that it covers,
    /// and the text that the edits below splice in is already written in terms of the catalog.
    prefixNames(*parse, rewriter, declarations.names);

    makeAnonymousSourcePathsAbsolute(*parse, rewriter, context.testDataDir);
    completeAnonymousSources(*parse, rewriter, context.sourceHost);

    const auto queryNumber = id.getRawValue();
    const auto suffix
        = resultDiscriminator.empty() ? fmt::format("{}", queryNumber) : fmt::format("{}_{}", resultDiscriminator, queryNumber);

    const auto sinks = requireSinks(*parse, sql);
    std::vector<std::optional<std::filesystem::path>> resultFiles;
    /// A query that lists one declared sink twice is submitted with its sinks as written, so the engine reports that error against
    /// this one query.
    if (not listsADeclaredSinkTwice(sinks))
    {
        for (const auto& [sinkIndex, sink] : sinks | views::enumerate)
        {
            auto [inlinedSink, resultFile] = sinkRewriter.inlineSink(*parse, sink, context.resultFile(sinkSuffix(suffix, sinkIndex)));
            rewriter.replace(sink->getStart(), sink->getStop(), inlinedSink);
            resultFiles.push_back(std::move(resultFile));
        }
    }

    std::vector<std::filesystem::path> inputFiles;
    for (auto* namedSource : findAll<AntlrSQLParser::NamedSourceContext>(parse->tree()))
    {
        /// A name that no identifier can hold matches no key, because every key comes from a name that this file declared.
        /// It is skipped rather than reported, so the query is still submitted and rejected when it runs.
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

    return RewrittenSql{.sql = rewriter.getText(), .resultFiles = std::move(resultFiles), .inputFiles = std::move(inputFiles)};
}

void Emitter::emitQuery(SelectStatement query)
{
    auto [sql, resultFiles, inputFiles] = emitSelect(query.sql, query.id, {});
    runnable.testCases.push_back(RewrittenTestCase{
        .action = RewrittenQuery{
            .sql = std::move(sql),
            .id = query.id,
            .resultFiles = std::move(resultFiles),
            .inputFiles = std::move(inputFiles),
            .expectation = std::move(query.expected)}});
}

void Emitter::emitExplain(ExplainStatement explain)
{
    SqlParse parse{explain.sql};
    /// The VISUAL format, which is also the default, centres the plan on the width of each operator label.
    /// The check restores the declared names afterwards, but the layout was computed for the prefixed ones.
    auto* format = findFirst<AntlrSQLParser::ExplainStatementContext>(parse.tree())->explainFormat();
    const auto isVisual = [](AntlrSQLParser::ExplainFormatContext* format)
    { return format->identifier() != nullptr and Identifier::parse(format->identifier()->getText()) == Identifier::parse("visual"); };
    if (format == nullptr or isVisual(format))
    {
        throw TestException(
            "An EXPLAIN check has to state FORMAT TEXT or FORMAT VERBOSE, because the VISUAL layout depends on the prefixed names: {}",
            explain.sql);
    }
    antlr4::TokenStreamRewriter rewriter{&parse.tokenStream()};
    prefixNames(parse, rewriter, declarations.names);

    /// The explained query binds and optimizes as a regular query does, so its sources need the same completion.
    makeAnonymousSourcePathsAbsolute(parse, rewriter, context.testDataDir);
    completeAnonymousSources(parse, rewriter, context.sourceHost);

    for (const auto& [sinkIndex, sink] : requireSinks(parse, explain.sql) | views::enumerate)
    {
        if (sink->identifier() == nullptr)
        {
            /// An EXPLAIN starts no query, so the file is never written, but the sink descriptor is still validated and needs a path.
            const auto candidate = context.resultFile(sinkSuffix(fmt::format("{}", explain.id.getRawValue()), sinkIndex));
            rewriter.replace(sink->getStart(), sink->getStop(), sinkRewriter.inlineSink(parse, sink, candidate).sql);
        }
    }

    runnable.testCases.push_back(RewrittenTestCase{
        .action = RewrittenExplain{.sql = rewriter.getText(), .id = explain.id, .expected = std::move(explain.expected)}});
}

void Emitter::emitDifferential(const DifferentialStatement& block)
{
    /// The second half needs its own result file name, because the halves share a query number.
    static constexpr auto SecondHalf = "DIFFERENTIAL";

    auto [firstSql, firstResultFiles, unmeasuredFirstInput] = emitSelect(block.firstSql, block.firstId, {});
    auto [secondSql, secondResultFiles, unmeasuredSecondInput] = emitSelect(block.secondSql, block.secondId, SecondHalf);
    /// The check compares one result file against another, so each half writes exactly one.
    const auto writesOneResult = [](const std::vector<std::optional<std::filesystem::path>>& resultFiles)
    { return resultFiles.size() == 1 and resultFiles.front().has_value(); };
    if (not writesOneResult(firstResultFiles) or not writesOneResult(secondResultFiles))
    {
        throw TestException("a differential query has to write one result to compare, into a single sink: {}", block.firstSql);
    }

    runnable.testCases.push_back(RewrittenTestCase{
        .action = RewrittenDifferential{
            .firstSql = std::move(firstSql),
            .firstId = block.firstId,
            .firstResultFile = std::move(*firstResultFiles.front()),
            .secondSql = std::move(secondSql),
            .secondId = block.secondId,
            .secondResultFile = std::move(*secondResultFiles.front())}});
}

}
