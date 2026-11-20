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

#include <Calcite/CalcitePlanner.hpp>
#include <filesystem>
#include <regex>
#include <SourceConfigSchemaRegistry.hpp>

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
#include <fmt/format.h>

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
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

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
    runnable.key = context.testFileKey.value();
    runnable.originalNames = this->declarations.names.collectOriginalNames();
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
    /// The worker resolves a relative path from its own working directory, not the test data directory.
    if (const std::filesystem::path declared{unquote(declaration.definition->modelPath->getText())}; declared.is_relative())
    {
        rewriter.replace(declaration.definition->modelPath, Sql::stringLiteral((context.testDataDir / declared).string()));
    }
    return PlainStatement{.sql = rewriter.getText()};
}

Emitter::RewrittenSql Emitter::emitSelect(const std::string& sql, const SystestQueryId id, const std::string_view resultDiscriminator)
{
    return emitCalciteSelect(sql, id, resultDiscriminator);
}

namespace
{
/// The anonymous sources of a Calcite query (TABLE(FILE(...)) etc., see AnonymousSources.java), completed as
/// completeAnonymousSources does for our dialect: a relative FILE_PATH is resolved against the test data, and a source
/// without HOST or FORMAT runs on the test's source host and reads CSV.
std::string completeSourceFunctions(const std::string& query, const std::filesystem::path& testDataDir, const Host& host)
{
    std::string result = query;
    static const std::regex filePath{R"((file_path\s*=>\s*)'([^']*)')", std::regex::icase};
    std::string withPaths;
    auto begin = std::sregex_iterator(result.begin(), result.end(), filePath);
    size_t last = 0;
    for (auto match = begin; match != std::sregex_iterator(); ++match)
    {
        withPaths += result.substr(last, match->position() - last);
        const std::filesystem::path declared{(*match)[2].str()};
        withPaths += (*match)[1].str() + "'" + (declared.is_relative() ? (testDataDir / declared).string() : declared.string()) + "'";
        last = match->position() + match->length();
    }
    withPaths += result.substr(last);
    result = std::move(withPaths);

    static const std::regex call{R"(TABLE\s*\(\s*([A-Za-z_][A-Za-z_0-9]*)\s*\()", std::regex::icase};
    std::smatch match;
    size_t from = 0;
    while (std::regex_search(result.cbegin() + static_cast<std::ptrdiff_t>(from), result.cend(), match, call))
    {
        const auto open = from + match.position() + match.length(); /// just after the function's '('
        from = open;
        if (not SourceConfigSchemaRegistry::instance().contains(match[1].str()))
        {
            continue; /// e.g. TUMBLE, MODEL_INFERENCE
        }
        size_t close = open;
        for (int depth = 1; close < result.size() and depth > 0; ++close)
        {
            depth += result[close] == '(' ? 1 : result[close] == ')' ? -1 : 0;
        }
        const auto arguments = result.substr(open, close - open);
        std::string missing;
        if (not std::regex_search(arguments, std::regex{R"(\bhost\s*=>)", std::regex::icase}))
        {
            missing += fmt::format("host => '{}', ", host.view());
        }
        if (not std::regex_search(arguments, std::regex{R"(\bformat\s*=>)", std::regex::icase}))
        {
            missing += "format => 'CSV', ";
        }
        result.insert(open, missing);
    }
    return result;
}
}

/// Calcite's dialect is not ours, so the query itself stays as written; its sources are offered to Calcite under their
/// declared names (see SystestBinder). Only the sink clause after INTO is ours: it is rewritten like any other sink, as
/// part of a dummy query in our dialect.
Emitter::RewrittenSql
Emitter::emitCalciteSelect(const std::string& sql, const SystestQueryId id, const std::string_view resultDiscriminator)
{
    std::pair<std::string, std::string> querySink;
    try
    {
        querySink = Calcite::splitSink(sql);
    }
    catch (const Exception&)
    {
        return RewrittenSql{.sql = sql, .resultFile = std::nullopt, .inputFiles = {}};
    }
    const auto& [writtenQuery, sinkClause] = querySink;
    const auto query = completeSourceFunctions(writtenQuery, context.testDataDir, context.sourceHost);
    const auto dummyQuery = std::string{"SELECT * FROM CALCITE_SINK_REWRITE INTO "};
    auto rewritten = emitNesSelect(dummyQuery + sinkClause, id, resultDiscriminator);
    if (not rewritten.sql.starts_with(dummyQuery))
    {
        return RewrittenSql{.sql = sql, .resultFile = std::nullopt, .inputFiles = {}};
    }
    rewritten.sql = fmt::format("{} INTO {}", query, rewritten.sql.substr(dummyQuery.size()));
    return rewritten;
}

Emitter::RewrittenSql Emitter::emitNesSelect(const std::string& sql, const SystestQueryId id, const std::string_view resultDiscriminator)
{
    std::unique_ptr<SqlParse> parse;
    try
    {
        parse = std::make_unique<SqlParse>(sql);
    }
    catch (const Exception&)
    {
        /// A statement that the parser rejects is submitted unchanged,
        /// so the syntax error fails this one query rather than the rewrite failing every query of the file.
        /// Many test files assert exactly that error.
        return RewrittenSql{.sql = sql, .resultFile = std::nullopt, .inputFiles = {}};
    }

    antlr4::TokenStreamRewriter rewriter{&parse->tokenStream()};

    /// Prefixing goes in first, because a later edit wins over an earlier one that it covers,
    /// and the text that the edits below splice in is already written in terms of the catalog.
    prefixNames(*parse, rewriter, declarations.names);

    const auto queryNumber = id.getRawValue();
    const auto suffix
        = resultDiscriminator.empty() ? fmt::format("{}", queryNumber) : fmt::format("{}_{}", resultDiscriminator, queryNumber);

    auto* sink = requireSingleSink(*parse, sql);
    auto [inlinedSink, resultFile] = sinkRewriter.inlineSink(*parse, sink, context.resultFile(suffix));
    rewriter.replace(sink->getStart(), sink->getStop(), inlinedSink);

    return RewrittenSql{.sql = rewriter.getText(), .resultFile = std::move(resultFile), .inputFiles = {}};
}

void Emitter::emitQuery(SelectStatement query)
{
    auto [sql, resultFile, inputFiles] = emitSelect(query.sql, query.id, {});
    runnable.testCases.push_back(
        RewrittenTestCase{
            .action = RewrittenQuery{
                .sql = std::move(sql),
                .id = query.id,
                .resultFile = std::move(resultFile),
                .inputFiles = std::move(inputFiles),
                .expected = std::move(query.expected)}});
}

void Emitter::emitExplain(ExplainStatement explain)
{
    /// As a query of the file (emitCalciteSelect): Calcite's dialect is not ours, the query stays as written.
    static const std::regex Head{R"(^\s*EXPLAIN\s+(?:FORMAT\s+([A-Za-z]+)\s+)?)", std::regex::icase};
    std::smatch head;
    if (not std::regex_search(explain.sql, head, Head) or not head[1].matched or Identifier::parse(head[1].str()) == Identifier::parse("visual"))
    {
        throw TestException(
            "An EXPLAIN check has to state FORMAT TEXT or FORMAT VERBOSE, because the VISUAL layout depends on the prefixed names: {}",
            explain.sql);
    }
    /// The statement ends in a semicolon, as the binder parses terminated statements; splitting off the sink drops it.
    const auto rewritten = emitCalciteSelect(explain.sql.substr(head.length()), explain.id, "explain");
    runnable.testCases.push_back(
        RewrittenTestCase{
            .action = RewrittenExplain{
                .sql = head.str() + rewritten.sql + ";", .id = explain.id, .expected = std::move(explain.expected)}});
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

    runnable.testCases.push_back(
        RewrittenTestCase{
            .action = RewrittenDifferential{
                .firstSql = std::move(firstSql),
                .firstId = block.firstId,
                .firstResultFile = std::move(*firstResultFile),
                .secondSql = std::move(secondSql),
                .secondId = block.secondId,
                .secondResultFile = std::move(*secondResultFile)}});
}

}
