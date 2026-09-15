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
#include <SystestBinder.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Config/Config.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifier.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/ParsedTestFile.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileBuilder.hpp>
#include <Parser/TestFilePartition.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/SourceRewriting.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Runner/DataStaging.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Pointers.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryId.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <WorkerCatalog.hpp>

namespace NES
{
namespace
{

/// The schema of the file that a checksum sink writes, which is not the schema of the rows going into the sink.
/// A function-local static, because the schema resolves data types through a registry that plugins populate at startup.
const Schema<UnqualifiedUnboundField, Ordered>& checksumSchema()
{
    static const Schema<UnqualifiedUnboundField, Ordered> ChecksumSchema{std::vector{
        UnqualifiedUnboundField{Identifier::parse("COUNT"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        UnqualifiedUnboundField{Identifier::parse("CHECKSUM"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
    return ChecksumSchema;
}

/// The schema of the rows in the query's result file, which the result check aligns the expected rows against.
Schema<UnqualifiedUnboundField, Ordered> sinkOutputSchema(const DistributedLogicalPlan& plan)
{
    const auto sinkOperator = plan.getGlobalPlan().getRootOperators().at(0).tryGetAs<SinkLogicalOperator>();
    INVARIANT(sinkOperator.has_value(), "The optimized plan should have a sink operator");
    const auto& descriptor = sinkOperator.value()->getSinkDescriptor(); /// NOLINT(bugprone-unchecked-optional-access)
    INVARIANT(descriptor.has_value(), "The sink operator should have a sink descriptor");
    if (Sql::sameName(descriptor->getSinkType(), Sql::Checksum))
    {
        return checksumSchema();
    }
    return *get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(descriptor->getSchema());
}

/// Throws the failure of a catalog submission, so one CREATE that the catalog rejects fails its whole test file.
template <typename Result>
void raiseOnError(std::expected<Result, Exception> result)
{
    if (not result.has_value())
    {
        throw std::move(result).error();
    }
}

/// A case whose plan cannot be compiled still becomes a query, so the run reports the failure against that case.
template <typename Compile>
std::expected<PlanInfo, Exception> compileOrError(Compile&& compile)
{
    try
    {
        return compile();
    }
    catch (Exception& e)
    {
        return std::unexpected{e};
    }
}

}

struct SystestBinder::Impl
{
    explicit Impl(const SystestConfiguration& config)
        : workingDir(config.workingDir.getValue())
        , testDataDir(config.testDataDir.getValue())
        , configDir(config.configDir.getValue())
        , discoverRoot(config.testDiscoverRoot.getValue())
        , clusterConfiguration(config.clusterConfig)
        , sourceCatalog(std::make_shared<SourceCatalog>())
        , sinkCatalog(std::make_shared<SinkCatalog>())
        , modelCatalog(std::make_shared<ModelCatalog>())
        , workerCatalog(std::make_shared<WorkerCatalog>())
        , sourceHandler{sourceCatalog, RequireHostConfig{}}
        , sinkHandler{sinkCatalog, RequireHostConfig{}}
        , modelHandler{modelCatalog}
        , statementBinder{sourceCatalog, [](auto&& plan) { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(plan)>(plan)); }}
        , queryOptimizer{
              config.queryOptimizerConfig.value_or(QueryOptimizerConfiguration{}),
              sourceCatalog,
              sinkCatalog,
              copyPtr(workerCatalog),
              modelCatalog}
    {
        for (const auto& [host, data, capacity, downstream, workerConfig] : clusterConfiguration.workers)
        {
            workerCatalog->addWorker(host, data, capacity, downstream, workerConfig);
        }
    }

    std::vector<RewrittenPart> rewrite(const DiscoveredTestFile& testfile)
    {
        SystestParser parser;
        parser.registerSubstitutionRule(
            {.keyword = "TESTDATA", .ruleFunction = [&](std::string& substitute) { substitute = testDataDir; }});
        parser.registerSubstitutionRule(
            {.keyword = "CONFIG/",
             .ruleFunction = [&](std::string& substitute)
             {
                 substitute = configDir;
                 if (!substitute.empty() && substitute.back() != '/')
                 {
                     substitute.push_back('/');
                 }
             }});
        parser.loadString(readTestFile(testfile.file));

        const ParsedTestFile parsedFile = [&]
        {
            try
            {
                return buildTestFile(parser, testfile.file);
            }
            catch (Exception& exception)
            {
                tryLogCurrentException();
                exception.what() += fmt::format("Could not successfully parse test file://{}", testfile.file.string());
                throw;
            }
        }();

        PRECONDITION(
            not clusterConfiguration.allowSourcePlacement.empty(),
            "Topology must list at least one worker in allow_source_placement to assign a default source host");
        PRECONDITION(
            not clusterConfiguration.allowSinkPlacement.empty(),
            "Topology must list at least one worker in allow_sink_placement to assign a default sink host");

        const DiscoveryRoot discoveryRoot{discoverRoot};
        auto parts = partitionByOverrides(parsedFile);
        const auto selected = testfile.enabledQueries.value_or(std::unordered_set<SystestQueryId>{});

        std::vector<RewrittenPart> rewritten;
        rewritten.reserve(parts.size());
        std::unordered_set<SystestQueryId> foundQueries;
        for (size_t index = 0; index < parts.size(); ++index)
        {
            auto& [overrides, file] = parts.at(index);
            const auto partKey = discoveryRoot.keyOf(testfile.file, index, parts.size());
            /// The rewrite consumes the part's statements. Only its overrides are read afterwards.
            auto runnable = rewriteTestFile(
                std::move(file),
                RewriteTarget{
                    .testFileKey = partKey,
                    .displayName = testfile.name().getRawValue(),
                    .workingDir = workingDir,
                    .testDataDir = testDataDir,
                    .sourceHost = clusterConfiguration.allowSourcePlacement.at(0),
                    .sinkHost = clusterConfiguration.allowSinkPlacement.at(0)});
            runnable.key = partKey.value();
            for (const auto& testCase : runnable.cases)
            {
                /// Both numbers of a differential block select it, so both count as found.
                foundQueries.insert(caseNumber(testCase));
                if (const auto* differential = std::get_if<RewrittenDifferential>(&testCase.action))
                {
                    foundQueries.insert(differential->secondId);
                }
            }
            keepSelectedCases(runnable, selected);
            /// When the selection drops every case of a part, nothing runs, so its data is not staged.
            if (runnable.cases.empty())
            {
                continue;
            }
            rewritten.push_back(RewrittenPart{.settings = overrides, .test = std::move(runnable)});
        }

        for (const auto badTestNumber : selected)
        {
            if (not foundQueries.contains(badTestNumber))
            {
                std::cerr << fmt::format(
                    "Warning: Query number {} specified via command line argument but not found in file://{}",
                    badTestNumber,
                    testfile.file.string());
            }
        }
        return rewritten;
    }

    /// Compiles one rewritten test file part: its setup goes into the catalogs, and each case becomes the statements to submit.
    /// The part key goes into each statement's distributed id, because the coordinator rejects two plans with one id and
    /// the parts of a file repeat its query numbers.
    [[nodiscard]] PlannedTest compile(const RunnableTestFile& runnable)
    {
        PlannedTest compiled;
        /// The staged SQL is what reaches the catalogs, so a served source includes the endpoint that its server bound.
        for (auto setup : runnable.setupStatements)
        {
            stage(setup, compiled.servers);
            submitToCatalogs(sqlOf(setup));
        }

        compiled.cases.reserve(runnable.cases.size());
        for (const auto& [action] : runnable.cases)
        {
            compiled.cases.push_back(std::visit(
                Overloaded{
                    [&](const RewrittenQuery& query) { return compileQuery(query, runnable.key); },
                    [&](const RewrittenDifferential& block) { return compileDifferential(block, runnable.key); },
                    [&](const RewrittenExplain& explain) { return compileExplain(explain); }},
                action));
        }
        return compiled;
    }

private:
    /// Stages a setup statement's data before the statement reaches the catalogs.
    /// A served source gets the endpoint that its server bound merged into its statement. The server thread has to
    /// outlive the queries that read from it, so it is kept with the test file's queries.
    static void stage(SetupStatement& setup, std::vector<std::jthread>& sourceThreads)
    {
        std::visit(
            Overloaded{
                [](const PlainStatement&) {},
                [](const StatementWithInlineData& withInline) { writeInlineData(withInline.data); },
                [&](StatementWithServedData& withServed)
                {
                    auto server = serve(std::move(withServed.data));
                    withServed.sql = addSourceOptions(withServed.sql, server.options);
                    sourceThreads.push_back(std::move(server.thread));
                }},
            setup);
    }

    void submitToCatalogs(const std::string& sql)
    {
        const auto binding = bindStatement(sql);
        std::visit(
            Overloaded{
                [&](const CreateLogicalSourceStatement& statement) { raiseOnError(sourceHandler(statement)); },
                [&](const CreatePhysicalSourceStatement& statement) { raiseOnError(sourceHandler(statement)); },
                [&](const CreateSinkStatement& statement) { raiseOnError(sinkHandler(statement)); },
                [&](const CreateModelStatement& statement) { raiseOnError(modelHandler(statement)); },
                [&](const auto&) { throw UnsupportedQuery("a setup statement has to declare a source, a sink, or a model: {}", sql); }},
            binding);
    }

    [[nodiscard]] NES::Statement bindStatement(const std::string& sql) const
    {
        const auto managedParser = AntlrSQLQueryParser::ManagedAntlrParser::create(sql);
        const auto parseResult = managedParser->parseSingle();
        if (not parseResult.has_value())
        {
            throw InvalidQuerySyntax("failed to parse the statement \"{}\"", replaceAll(sql, "\n", " "));
        }
        auto binding = statementBinder.bind(parseResult.value().get());
        if (not binding.has_value())
        {
            throw InvalidQuerySyntax("failed to bind the statement \"{}\"", replaceAll(sql, "\n", " "));
        }
        return std::move(binding).value();
    }

    /// Compiles one query into the plan to submit.
    [[nodiscard]] std::vector<PlannedStatement> compileQuery(const RewrittenQuery& query, const std::string& partKey) const
    {
        auto plan = compileOrError([&] { return optimizeInto(query.sql, fmt::format("{}:{}", partKey, query.id.getRawValue())); });
        return {PlannedStatement{.plan = std::move(plan), .explained = std::nullopt}};
    }

    /// Compiles both halves of a differential block, which run one after the other and are compared against each other.
    /// A half that does not compile makes the block fail, so the first failure is reported for the pair.
    [[nodiscard]] std::vector<PlannedStatement> compileDifferential(const RewrittenDifferential& block, const std::string& partKey) const
    {
        try
        {
            auto first = optimizeInto(block.firstSql, fmt::format("{}:{}", partKey, block.firstId.getRawValue()));
            auto second = optimizeInto(block.secondSql, fmt::format("{}:{}-differential", partKey, block.firstId.getRawValue()));
            std::vector<PlannedStatement> compiled;
            compiled.push_back(PlannedStatement{.plan = std::move(first), .explained = std::nullopt});
            compiled.push_back(PlannedStatement{.plan = std::move(second), .explained = std::nullopt});
            return compiled;
        }
        catch (Exception& exception)
        {
            return {PlannedStatement{.plan = std::unexpected{exception}, .explained = std::nullopt}};
        }
    }

    /// Answers an EXPLAIN here rather than at run time, because only this component holds the optimizer that the
    /// OPTIMIZED and DISTRIBUTED stages need.
    /// It never reaches a worker and has no plan to run.
    [[nodiscard]] std::vector<PlannedStatement> compileExplain(const RewrittenExplain& explain) const
    {
        std::optional<std::string> explained;
        std::expected<PlanInfo, Exception> plan = std::unexpected{TestException("an EXPLAIN is not executed and has no plan")};
        try
        {
            explained = explainOutput(explain.sql);
        }
        catch (Exception& exception)
        {
            plan = std::unexpected{exception};
        }
        return {PlannedStatement{.plan = std::move(plan), .explained = std::move(explained)}};
    }

    /// Parses and optimizes one statement under the given distributed query id, which correlates it with its answer.
    [[nodiscard]] PlanInfo optimizeInto(const std::string& sql, const std::string& queryId) const
    {
        auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
        plan.setQueryId(QueryId::createDistributed(DistributedQueryId(queryId)));
        auto optimized = queryOptimizer.optimize(plan);
        auto schema = sinkOutputSchema(optimized);
        return PlanInfo{.plan = std::move(optimized), .sinkOutputSchema = std::move(schema)};
    }

    [[nodiscard]] std::string explainOutput(const std::string& sql) const
    {
        const auto binding = bindStatement(sql);
        const auto* explainStatement = std::get_if<ExplainQueryStatement>(&binding);
        if (explainStatement == nullptr)
        {
            throw UnsupportedQuery("expected an EXPLAIN statement, but got: \"{}\"", replaceAll(sql, "\n", " "));
        }
        return computeExplainOutput(*explainStatement, queryOptimizer);
    }

    std::filesystem::path workingDir;
    std::filesystem::path testDataDir;
    std::filesystem::path configDir;
    std::filesystem::path discoverRoot;
    SystestClusterConfiguration clusterConfiguration;

    /// One catalog set for the whole invocation, which the rewriter's name qualification keeps collision-free.
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
    std::shared_ptr<ModelCatalog> modelCatalog;
    SharedPtr<WorkerCatalog> workerCatalog;

    SourceStatementHandler sourceHandler;
    SinkStatementHandler sinkHandler;
    ModelStatementHandler modelHandler;
    StatementBinder statementBinder;
    QueryOptimizer queryOptimizer;
};

SystestBinder::SystestBinder(const SystestConfiguration& config) : impl(std::make_unique<Impl>(config))
{
}

std::vector<RewrittenPart> SystestBinder::rewrite(const DiscoveredTestFile& testfile)
{
    return impl->rewrite(testfile);
}

PlannedTest SystestBinder::plan(const RunnableTestFile& runnable)
{
    return impl->compile(runnable);
}

SystestBinder::~SystestBinder() = default;
}
