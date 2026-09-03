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

#include <Rewriter/SqlRewriter.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <AntlrSQLParser.h>
#include <TokenStreamRewriter.h>
#include <Identifiers/Identifier.hpp>
#include <Model/RunnableTest.hpp>
#include <Parser/TestFileParser.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/ParsedStatement.hpp>
#include <Rewriter/SinkRewriting.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/SystestQueryId.hpp>
#include <Parser/SystestParser.hpp>

namespace NES
{
namespace
{

/// The file a physical source with attached data reads.
/// A test that declared its rows inline instead of pointing at a file also gets the CSV to write there.
/// The ordinal keeps the generated file name unique within the test file.
struct AttachedFile
{
    std::filesystem::path file;
    std::optional<InlineData> inlineData;
};

AttachedFile attachedFileFor(const AttachedData& attach, const RewriteTarget& target, const size_t ordinal)
{
    const auto& [kind, rows] = attach;
    if (kind == TestDataIngestionType::INLINE)
    {
        const auto file = target.workingDir / "sources" / fmt::format("{}_{}.csv", target.testFileKey, ordinal);
        return {.file = file, .inlineData = InlineData{.path = file, .rows = rows}};
    }
    return {.file = target.testDataDir / rows.at(0), .inlineData = std::nullopt};
}

/// Returns whether a source of this type reads over a connection instead of from a file.
/// Such a source takes an endpoint instead of a path, and needs a server sending its data while it reads.
bool readsFromSocket(const std::string_view sourceType)
{
    return Identifier::parse(std::string{sourceType}) == Identifier::parse(std::string{Sql::Tcp});
}

/// Returns the data a server sends for a source: the rows the test declared, or the file it pointed at.
ServedData servedDataFor(const AttachedData& attach, const RewriteTarget& target)
{
    const auto& [kind, rows] = attach;
    if (kind == TestDataIngestionType::INLINE)
    {
        return {.content = rows};
    }
    return {.content = target.testDataDir / rows.at(0)};
}

/// Returns the options the test wrote on a physical source, or null when it wrote none.
AntlrSQLParser::NamedConfigExpressionSeqContext* declaredOptions(AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition)
{
    const auto* clause = definition->optionsClause();
    return clause != nullptr ? clause->options : nullptr;
}

/// Replaces a physical source's SET clause with the given options, or adds the clause when the source has none.
/// The grammar allows only one SET clause per physical source, so appending a second one is not an option.
std::string spliceSetClause(
    ParsedStatement& parsed, AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition, const std::string_view setClause)
{
    antlr4::TokenStreamRewriter rewriter{&parsed.tokenStream()};
    if (const auto* clause = definition->optionsClause(); clause != nullptr)
    {
        rewriter.replace(clause->getStart(), clause->getStop(), std::string{setClause});
    }
    else
    {
        rewriter.insertAfter(definition->getStop(), fmt::format(" {}", setClause));
    }
    return rewriter.getText();
}

/// Builds the SET clause of a physical source.
/// The clause pins the source to one worker, so the coordinator can place it.
/// It points at the file with the attached data when there is one, and defaults to a CSV input format unless the test chose one.
/// The options the test wrote come last, unchanged.
std::string physicalSourceOptions(
    ParsedStatement& parsed,
    AntlrSQLParser::NamedConfigExpressionSeqContext* declared,
    const std::optional<std::filesystem::path>& dataFile,
    const RewriteTarget& target)
{
    std::vector<std::string> options;
    if (dataFile.has_value())
    {
        options.push_back(Sql::option(Sql::Source, Sql::FilePath, dataFile->string()));
    }
    options.push_back(Sql::option(Sql::Source, Sql::Host, target.sourceHost.view()));
    if (not declaresOption(declared, Sql::InputFormatter, Sql::Type))
    {
        options.push_back(Sql::option(Sql::InputFormatter, Sql::Type, Sql::Csv));
    }
    if (const auto declaredText = parsed.textOf(declared); not declaredText.empty())
    {
        options.push_back(declaredText);
    }
    return Sql::setClause(Sql::optionList(options));
}

/// What a CREATE declares, which decides what the rewriter emits in its place.
/// Only a physical source takes attached data, so only that alternative holds a field for it.
struct CreateLogicalSourceStatement
{
    AntlrSQLParser::CreateLogicalSourceDefinitionContext* definition;
};

struct CreatePhysicalSourceStatement
{
    AntlrSQLParser::CreatePhysicalSourceDefinitionContext* definition;
    /// Absent when the source produces its own rows from its own options instead of reading attached data.
    std::optional<AttachedData> attached;
};

struct CreateSinkStatement
{
    AntlrSQLParser::CreateSinkDefinitionContext* definition;
};

struct CreateModelStatement
{
    AntlrSQLParser::CreateModelDefinitionContext* definition;
};

using Declaration = std::variant<CreateLogicalSourceStatement, CreatePhysicalSourceStatement, CreateSinkStatement, CreateModelStatement>;

/// Reads what a CREATE declares from its parse tree, and binds attached data only to a physical source.
/// The test file format allows an ATTACH after any CREATE, because the parser matches line prefixes and never reads the statement.
/// This rejects a misplaced ATTACH.
Declaration classify(const ParsedStatement& parsed, const Systest::CreateStatement& create)
{
    if (auto* physicalSource = findFirst<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parsed.tree()))
    {
        return CreatePhysicalSourceStatement{.definition = physicalSource, .attached = create.attach};
    }
    if (create.attach.has_value())
    {
        throw TestException("ATTACH supplies the data of a physical source, but this statement declares something else: {}", create.sql);
    }

    if (auto* logicalSource = findFirst<AntlrSQLParser::CreateLogicalSourceDefinitionContext>(parsed.tree()))
    {
        return CreateLogicalSourceStatement{.definition = logicalSource};
    }
    if (auto* sink = findFirst<AntlrSQLParser::CreateSinkDefinitionContext>(parsed.tree()))
    {
        return CreateSinkStatement{.definition = sink};
    }
    if (auto* model = findFirst<AntlrSQLParser::CreateModelDefinitionContext>(parsed.tree()))
    {
        return CreateModelStatement{.definition = model};
    }
    throw TestException("Unsupported CREATE statement: {}", create.sql);
}

/// Builds the model statement to submit, pointing at the file that holds the model.
/// A test gives that path relative to the test data directory, but the worker loading it resolves a relative path against its own
/// working directory, so the path has to be absolute.
RunnableCreateStatement
modelStatement(ParsedStatement& parsed, const CreateModelStatement& declaration, const RewriteTarget& target, const QualifiedNames& names)
{
    antlr4::TokenStreamRewriter rewriter{&parsed.tokenStream()};
    if (const std::filesystem::path declared{unquote(declaration.definition->modelPath->getText())}; declared.is_relative())
    {
        rewriter.replace(declaration.definition->modelPath, Sql::stringLiteral((target.testDataDir / declared).string()));
    }
    return RunnableCreateStatement{.sql = rewriteIdentifiers(rewriter.getText(), names), .staged = std::nullopt};
}

/// The file a source reading from a socket is served, when the test pointed at one rather than declaring its rows inline.
/// A measurement counts what a source reads, and it reads the same bytes whether they arrive over a socket or not.
std::optional<std::filesystem::path> servedFile(const CreatePhysicalSourceStatement& declaration, const RewriteTarget& target)
{
    if (not declaration.attached.has_value() or declaration.attached->kind != TestDataIngestionType::FILE
        or not readsFromSocket(declaration.definition->type->getText()))
    {
        return std::nullopt;
    }
    return target.testDataDir / declaration.attached->rows.at(0);
}

/// Builds the physical source statement to submit, plus the CSV to write when the test declared its rows inline.
/// The ordinal keeps the generated data file unique within the test file.
RunnableCreateStatement physicalSourceStatement(
    ParsedStatement& parsed,
    const CreatePhysicalSourceStatement& declaration,
    const RewriteTarget& target,
    const size_t ordinal,
    const QualifiedNames& names,
    std::optional<std::filesystem::path>& readFile)
{
    /// A source that reads a file takes the path of its attached data.
    /// A source that reads a socket takes no path, because a server sends it the data and the port is known only once that server binds.
    /// A source that produces its own data takes neither.
    std::optional<std::filesystem::path> dataFile;
    std::optional<StagedData> staged;
    if (declaration.attached.has_value())
    {
        if (readsFromSocket(declaration.definition->type->getText()))
        {
            staged = servedDataFor(*declaration.attached, target);
        }
        else
        {
            auto [file, inlineData] = attachedFileFor(*declaration.attached, target, ordinal);
            dataFile = std::move(file);
            if (inlineData.has_value())
            {
                staged = std::move(*inlineData);
            }
        }
    }

    readFile = dataFile.has_value() ? dataFile : servedFile(declaration, target);
    const auto setClause = physicalSourceOptions(parsed, declaredOptions(declaration.definition), dataFile, target);
    return RunnableCreateStatement{
        .sql = rewriteIdentifiers(spliceSetClause(parsed, declaration.definition, setClause), names), .staged = std::move(staged)};
}

/// Rewrites the data file path of a source written into a query to an absolute path under the test data directory.
/// A test gives that path relative to the test data directory, as it does everywhere else.
/// The worker resolves a relative path against its own working directory, so only an absolute path reaches the same file.
void makeAnonymousSourcePathsAbsolute(
    const ParsedStatement& parsed, antlr4::TokenStreamRewriter& rewriter, const std::filesystem::path& testDataDir)
{
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parsed.tree()))
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

/// Pins a source written into a query to the worker the run places sources on.
/// The coordinator resolves an omitted host to the worker it answers as the default, and a run placed on a topology answers none, so
/// such a source would have nowhere to run.
/// A source that states its own host keeps it.
void placeAnonymousSources(const ParsedStatement& parsed, antlr4::TokenStreamRewriter& rewriter, const Host& host)
{
    for (const auto* source : findAll<AntlrSQLParser::AnonymousSourceContext>(parsed.tree()))
    {
        const auto declared = source->parameters->namedConfigExpression();
        if (std::ranges::any_of(declared, [](auto* option) { return namesOption(option, Sql::Source, Sql::Host); }))
        {
            continue;
        }
        rewriter.insertAfter(source->parameters->getStop(), fmt::format(", {}", Sql::option(Sql::Source, Sql::Host, host.view())));
    }
}

}

SqlRewriter::SqlRewriter(RewriteTarget target) : target(std::move(target))
{
}

/// One CREATE statement, its parse tree, and what that tree says the statement declares.
/// A pointer holds the parse, because a parse refers to its own members and cannot be moved.
/// The declaration points into that parse and the statement into the caller's test file, so both outlive one rewrite.
struct SqlRewriter::PreparedCreate
{
    const Systest::CreateStatement* create;
    std::unique_ptr<ParsedStatement> parsed;
    Declaration declaration;
};

void SqlRewriter::declareNames(PreparedCreate& prepared, NameRegistry& registry)
{
    std::visit(
        Overloaded{
            [&](const CreateLogicalSourceStatement& declaration)
            {
                /// Qualifying the logical source name keeps it apart from the names of other test files.
                registry.declare(declaration.definition->sourceName->getText());
            },
            /// A physical source declares no name of its own.
            /// It references a logical source that another statement declares.
            [](const CreatePhysicalSourceStatement&) {},
            [&](const CreateModelStatement& declaration)
            {
                /// A query that infers with a model refers to it, so a model qualifies like a source.
                registry.declare(declaration.definition->modelName->getText());
            },
            [&](const CreateSinkStatement& declaration)
            {
                /// A test file holding an EXPLAIN declares its sinks, so the plan prints the name the test gave one.
                /// An inlined sink prints as its type instead.
                /// A declared sink name qualifies like any other name.
                if (declaresSinks)
                {
                    registry.declare(declaration.definition->sinkName->getText());
                }
                /// The rewriter inlines a sink into the query that writes to it, so this holds its type and schema and emits nothing.
                sinkByName.emplace(
                    Identifier::parse(declaration.definition->sinkName->getText()),
                    SinkDefinition{
                        .type = declaration.definition->type->getText(),
                        .schema = prepared.parsed->textOf(declaration.definition->schemaDefinition())});
            }},
        prepared.declaration);
}

void SqlRewriter::emitCreate(PreparedCreate& prepared, const QualifiedNames& names)
{
    std::visit(
        Overloaded{
            [&](const CreateLogicalSourceStatement&)
            {
                runnable.createStmts.push_back(
                    RunnableCreateStatement{.sql = rewriteIdentifiers(prepared.create->sql, names), .staged = std::nullopt});
            },
            [&](const CreatePhysicalSourceStatement& declaration)
            {
                std::optional<std::filesystem::path> readFile;
                runnable.createStmts.push_back(
                    physicalSourceStatement(*prepared.parsed, declaration, target, sourceOrdinal++, names, readFile));
                if (readFile.has_value())
                {
                    /// Keyed by the qualified name, because a query is parsed after its identifiers were substituted and
                    /// refers to the source by the spelling the catalog sees.
                    const auto declared = declaration.definition->logicalSource->getText();
                    recordInputFile(Identifier::parse(names.qualified(declared).value_or(declared)), *readFile);
                }
            },
            [&](const CreateModelStatement& declaration)
            { runnable.createStmts.push_back(modelStatement(*prepared.parsed, declaration, target, names)); },
            [&](const CreateSinkStatement& declaration)
            {
                /// A test file that inlines its sinks emits nothing here, because the declaring phase already held this one.
                /// A test file holding an EXPLAIN needs the sink to exist for the plan to print it.
                if (declaresSinks)
                {
                    runnable.createStmts.push_back(RunnableCreateStatement{
                        .sql = declaredSinkStatement(*prepared.parsed, declaration.definition, target, names), .staged = std::nullopt});
                }
            }},
        prepared.declaration);
}

RunnableQuery SqlRewriter::rewriteQuery(
    const std::string& sql,
    const SystestQueryId id,
    Expectation expected,
    const QualifiedNames& names,
    const std::string_view resultDiscriminator)
{
    /// Qualifying the source references comes first, because everything below locates and replaces text in the qualified statement.
    /// Substituting names reads tokens rather than a parse tree, so it also works on a statement that does not parse.
    auto qualified = rewriteIdentifiers(sql, names);

    /// A statement the parser rejects goes to the coordinator unchanged, which reports the syntax error against this one query.
    /// Several tests assert exactly that error, and a statement without a parse tree has nothing to inline into.
    /// Such a query never runs, so it writes no result file.
    std::unique_ptr<ParsedStatement> parse;
    try
    {
        parse = std::make_unique<ParsedStatement>(qualified);
    }
    catch (const Exception&)
    {
        sourcesPerQuery.emplace_back();
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
    auto [inlinedSink, resultFile, rowsAreJson] = inlineSink(parsed, sink, sinkByName, candidateResultFile, target);
    rewriter.replace(sink->getStart(), sink->getStop(), inlinedSink);

    /// The query name maps its submission and its result back to this test file.
    const auto queryName = fmt::format("{}:{}", target.testFileKey, queryNumber);
    rewriter.insertAfter(sink->getStop(), fmt::format(" {}", Sql::setClause(Sql::option(Sql::Query, Sql::Name, queryName))));

    const auto references = findAll<AntlrSQLParser::NamedSourceContext>(parsed.tree());
    sourcesPerQuery.push_back(
        references | std::views::transform([](auto* reference) { return Identifier::parse(reference->multipartIdentifier()->getText()); })
        | std::ranges::to<std::vector<Identifier>>());

    /// How the rows are written is a property of the inlined sink, so the expectation gets it here rather than from the parser.
    if (auto* rows = std::get_if<ExpectedRows>(&expected))
    {
        rows->rowsAreJson = rowsAreJson;
    }

    return RunnableQuery{
        .sql = rewriter.getText(), .id = id, .resultFile = std::move(resultFile), .inputFiles = {}, .expectation = std::move(expected)};
}

void SqlRewriter::emitExplain(const Systest::ExplainStatement& explain, const QualifiedNames& names)
{
    auto qualified = rewriteIdentifiers(explain.sql, names);
    ParsedStatement parsed{qualified};

    /// A sink the test declared keeps its name, because the expected plan prints that name and the emitted declaration makes it resolve.
    /// A sink written into the statement is inlined as it is for a query, and the plan prints it as the operator the optimizer
    /// has not bound yet.
    if (auto* sink = requireSingleSink(parsed, explain.sql); sink->identifier() == nullptr)
    {
        antlr4::TokenStreamRewriter rewriter{&parsed.tokenStream()};
        const auto candidate = target.workingDir / fmt::format("{}_{}.csv", target.testFileKey, explain.id.getRawValue());
        auto [inlined, resultFile, rowsAreJson] = inlineSink(parsed, sink, sinkByName, candidate, target);
        rewriter.replace(sink->getStart(), sink->getStop(), inlined);
        qualified = rewriter.getText();
    }

    /// An EXPLAIN starts no query, so it writes no result file and takes no query name.
    sourcesPerQuery.emplace_back();
    runnable.cases.push_back(RunnableCase{
        .action = RunnableQuery{
            .sql = std::move(qualified),
            .id = explain.id,
            .resultFile = std::nullopt,
            .inputFiles = {},
            .expectation = ExpectedPlan{.lines = explain.expected, .qualifyingPrefix = target.testFileKey + "_"}},
        .runsAfterPrevious = false});
}

void SqlRewriter::emitDifferential(const Systest::DifferentialStatement& block, const QualifiedNames& names)
{
    /// The second half needs its own result file name, because the halves share one query number.
    static constexpr std::string_view secondHalf = "_DIFFERENTIAL";

    /// Neither half carries an expectation of its own, so the placeholder passed here is discarded with the rest of
    /// the per-query shell below.
    auto first = rewriteQuery(block.firstSql, block.firstId, ExpectedRows{}, names, {});
    auto second = rewriteQuery(block.secondSql, block.secondId, ExpectedRows{}, names, secondHalf);
    if (not first.resultFile.has_value() or not second.resultFile.has_value())
    {
        throw TestException("a differential query has to write a result to compare: {}", block.firstSql);
    }

    /// Rewriting each half recorded its sources separately, and the block is one case, so the two records merge.
    auto secondSources = std::move(sourcesPerQuery.back());
    sourcesPerQuery.pop_back();
    std::ranges::move(secondSources, std::back_inserter(sourcesPerQuery.back()));

    runnable.cases.push_back(RunnableCase{
        .action = RunnableDifferential{
            .firstSql = std::move(first.sql),
            .firstId = block.firstId,
            .firstResultFile = std::move(*first.resultFile),
            .secondSql = std::move(second.sql),
            .secondId = block.secondId,
            .secondResultFile = std::move(*second.resultFile),
            .inputFiles = {}},
        .runsAfterPrevious = block.sequential});
}

std::string addSourceOptions(const std::string& sql, const std::vector<std::string>& options)
{
    ParsedStatement parsed{sql};
    auto* definition = findFirst<AntlrSQLParser::CreatePhysicalSourceDefinitionContext>(parsed.tree());
    if (definition == nullptr)
    {
        throw TestException("Only a physical source takes source options, but this statement declares something else: {}", sql);
    }

    auto merged = options;
    if (const auto declaredText = parsed.textOf(declaredOptions(definition)); not declaredText.empty())
    {
        merged.push_back(declaredText);
    }
    return spliceSetClause(parsed, definition, Sql::setClause(Sql::optionList(merged)));
}

RunnableTest SqlRewriter::rewrite(const TestFile& testFile)
{
    /// One statement of the test file, ready for the phases that follow, in the order the test wrote them.
    /// A CREATE is parsed and classified here, because both later phases need that.
    /// A query is only referenced, because rewriting it needs the sealed names, which do not exist yet.
    using PreparedStatement = std::
        variant<PreparedCreate, const Systest::QueryStatement*, const Systest::DifferentialStatement*, const Systest::ExplainStatement*>;

    runnable.name = target.displayName;

    /// A sink an EXPLAIN writes into has to exist for the plan to print it, so a test file holding an EXPLAIN declares its sinks.
    /// No test file mixes declared and inlined sinks.
    declaresSinks = std::ranges::any_of(
        testFile.statements, [](const auto& statement) { return std::holds_alternative<Systest::ExplainStatement>(statement); });
    std::vector<PreparedStatement> prepared;
    prepared.reserve(testFile.statements.size());
    for (const auto& statement : testFile.statements)
    {
        prepared.push_back(std::visit(
            Overloaded{
                [](const Systest::CreateStatement& create) -> PreparedStatement
                {
                    auto parsed = std::make_unique<ParsedStatement>(create.sql);
                    auto declaration = classify(*parsed, create);
                    return PreparedCreate{.create = &create, .parsed = std::move(parsed), .declaration = std::move(declaration)};
                },
                [](const Systest::QueryStatement& query) -> PreparedStatement { return &query; },
                [](const Systest::DifferentialStatement& block) -> PreparedStatement { return &block; },
                [](const Systest::ExplainStatement& explain) -> PreparedStatement { return &explain; }},
            statement));
    }

    /// Every catalog-visible name of the file is registered before any statement is rewritten against it.
    /// Rewriting substitutes only registered names, so this order makes the result independent of where a name is declared.
    NameRegistry registry{target.testFileKey};
    for (auto& statement : prepared)
    {
        if (auto* create = std::get_if<PreparedCreate>(&statement))
        {
            declareNames(*create, registry);
        }
    }
    const auto names = std::move(registry).seal();

    /// Emitting in file order keeps the setup statements in the order the test wrote them, and keeps the data file numbering stable.
    for (auto& statement : prepared)
    {
        std::visit(
            Overloaded{
                [&](PreparedCreate& create) { emitCreate(create, names); },
                [&](const Systest::QueryStatement* query)
                {
                    auto rewritten = rewriteQuery(query->sql, query->id, query->expected, names, {});
                    runnable.cases.push_back(RunnableCase{.action = std::move(rewritten), .runsAfterPrevious = query->sequential});
                },
                [&](const Systest::DifferentialStatement* block) { emitDifferential(*block, names); },
                [&](const Systest::ExplainStatement* explain) { emitExplain(*explain, names); }},
            statement);
    }
    resolveInputFiles();
    return std::move(runnable);
}

void SqlRewriter::recordInputFile(const Identifier& logicalSource, const std::filesystem::path& dataFile)
{
    inputFilesBySource[logicalSource].push_back(dataFile);
}

void SqlRewriter::resolveInputFiles()
{
    INVARIANT(
        sourcesPerQuery.size() == runnable.cases.size(),
        "every case has to have recorded the sources it names, but {} did of {}",
        sourcesPerQuery.size(),
        runnable.cases.size());

    for (size_t index = 0; index < runnable.cases.size(); ++index)
    {
        auto& inputFiles = std::visit(
            [](auto& action) -> std::vector<std::filesystem::path>& { return action.inputFiles; }, runnable.cases.at(index).action);
        for (const auto& source : sourcesPerQuery.at(index))
        {
            if (const auto files = inputFilesBySource.find(source); files != inputFilesBySource.end())
            {
                std::ranges::copy(files->second, std::back_inserter(inputFiles));
            }
        }
    }
}

}
