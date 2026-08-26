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
#include <exception>
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
#include <Model/RewrittenTest.hpp>
#include <Model/TestFile.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Parser/SystestParser.hpp>
#include <Parser/TestFileParser.hpp>
#include <Parser/TestFilePartition.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Rewriter/SqlRewriter.hpp>
#include <Runner/DataStaging.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/Logger.hpp>
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

namespace NES::Systest
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
    if (toUpperCase(descriptor->getSinkType()) == "CHECKSUM")
    {
        return checksumSchema();
    }
    return *get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(descriptor->getSchema());
}

/// Drops the cases a run did not select, keeping every one the selection depends on.
/// A case that has to follow the one above it needs that one to still be there, so the chain stays contiguous and the
/// dependency still points at it.
/// A differential block is one case covering both its query numbers, so selecting either number keeps the block.
void keepSelectedQueries(RewrittenTest& runnable, const std::unordered_set<SystestQueryId>& selected)
{
    if (selected.empty())
    {
        return;
    }

    std::vector<bool> keep(runnable.cases.size(), false);
    for (size_t index = 0; index < runnable.cases.size(); ++index)
    {
        keep.at(index) = std::visit(
            Overloaded{
                [&](const RewrittenQuery& query) { return selected.contains(query.id); },
                [&](const RewrittenDifferential& differential)
                { return selected.contains(differential.firstId) or selected.contains(differential.secondId); }},
            runnable.cases.at(index).action);
    }
    for (size_t index = runnable.cases.size(); index > 1; --index)
    {
        if (keep.at(index - 1) and runnable.cases.at(index - 1).runsAfterPrevious)
        {
            keep.at(index - 2) = true;
        }
    }

    size_t index = 0;
    std::erase_if(runnable.cases, [&](const RewrittenCase&) { return not keep.at(index++); });
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

        const TestFile parsedFile = [&]
        {
            try
            {
                return parseTestFile(parser, testfile.file);
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

        const auto testFileKey = getTestFileKey(testfile.file, discoverRoot);
        const auto parts = partitionBySettings(parsedFile);

        std::vector<RewrittenPart> rewritten;
        rewritten.reserve(parts.size());
        std::unordered_set<SystestQueryId> foundQueries;
        for (size_t index = 0; index < parts.size(); ++index)
        {
            const auto& part = parts.at(index);
            const SqlRewriter rewriter{RewriteTarget{
                .testFileKey = getTestFilePartKey(testFileKey, index, parts.size()),
                .displayName = testfile.name(),
                .workingDir = workingDir,
                .testDataDir = testDataDir,
                .sourceHost = clusterConfiguration.allowSourcePlacement.at(0),
                .sinkHost = clusterConfiguration.allowSinkPlacement.at(0)}};
            auto runnable = rewriter.rewrite(part.file);
            runnable.variant = static_cast<uint32_t>(index);
            for (const auto& testCase : runnable.cases)
            {
                foundQueries.insert(caseNumber(testCase));
            }
            keepSelectedQueries(runnable, testfile.onlyEnableQueriesWithTestQueryNumber);
            rewritten.push_back(RewrittenPart{.settings = part.settings, .test = std::move(runnable)});
        }

        for (const auto badTestNumber : testfile.onlyEnableQueriesWithTestQueryNumber)
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

    [[nodiscard]] PlannedTest compile(const RewrittenTest& runnable)
    {
        PlannedTest compiled;
        /// The staged SQL is what reaches the catalogs, so a source fed by a server carries the endpoint it bound.
        for (auto create : runnable.createStmts)
        {
            stage(create, compiled.servers);
            submitToCatalogs(create.sql);
        }

        compiled.cases.reserve(runnable.cases.size());
        for (const auto& testCase : runnable.cases)
        {
            compiled.cases.push_back(std::visit(
                Overloaded{
                    [&](const RewrittenQuery& query) { return compileQuery(query, runnable.name); },
                    [&](const RewrittenDifferential& block) { return compileDifferential(block, runnable.name); }},
                testCase.action));
        }
        return compiled;
    }

private:
    /// Puts the data that a setup statement stages in place before the statement reaches the catalogs.
    /// A source that a server feeds gets the endpoint that the server bound merged into its statement, and the server
    /// thread has to outlive the queries reading from it, so it goes with the test file's queries.
    static void stage(RewrittenCreateStatement& create, std::vector<std::jthread>& sourceThreads)
    {
        if (not create.staged.has_value())
        {
            return;
        }
        std::visit(
            Overloaded{
                [](const InlineData& inlineData) { writeInlineData(inlineData); },
                [&](const ServedData& servedData)
                {
                    auto server = serve(servedData);
                    create.sql = addSourceOptions(create.sql, server.options);
                    sourceThreads.push_back(std::move(server.thread));
                }},
            *create.staged);
    }

    /// Binds one rewritten CREATE statement and submits it to the catalog that holds its kind.
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

    /// Compiles one query into the plan to submit, or answers it here when it is an EXPLAIN.
    [[nodiscard]] std::vector<PlannedStatement> compileQuery(const RewrittenQuery& query, const std::string& testName) const
    {
        std::optional<std::string> explained;
        auto plan = [&]() -> std::expected<PlanInfo, Exception>
        {
            try
            {
                /// An EXPLAIN is answered here rather than at run time, because only this component holds the optimizer
                /// that the OPTIMIZED and DISTRIBUTED stages need. It never reaches a worker and has no plan to run.
                if (std::holds_alternative<ExpectedPlan>(query.expectation))
                {
                    explained = explainOutput(query.sql);
                    return std::unexpected{TestException("an EXPLAIN is not executed and has no plan")};
                }
                return optimizeInto(query.sql, fmt::format("{}:{}", testName, query.id.getRawValue()));
            }
            catch (Exception& exception)
            {
                return std::unexpected{exception};
            }
        }();
        return {PlannedStatement{.plan = std::move(plan), .explained = std::move(explained)}};
    }

    /// Compiles both halves of a differential block, which run one after the other and are compared against each other.
    /// A half that does not compile makes the block fail, so the failure of the first is reported for the pair.
    [[nodiscard]] std::vector<PlannedStatement> compileDifferential(const RewrittenDifferential& block, const std::string& testName) const
    {
        try
        {
            auto first = optimizeInto(block.firstSql, fmt::format("{}:{}", testName, block.firstId.getRawValue()));
            auto second = optimizeInto(block.secondSql, fmt::format("{}:{}-differential", testName, block.firstId.getRawValue()));
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

    /// Parses and optimizes one statement under the given distributed query id, which correlates it with its answer.
    [[nodiscard]] PlanInfo optimizeInto(const std::string& sql, const std::string& queryId) const
    {
        auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
        plan.setQueryId(QueryId::createDistributed(DistributedQueryId(queryId)));
        auto optimized = queryOptimizer.optimize(plan);
        auto schema = sinkOutputSchema(optimized);
        return PlanInfo{.plan = std::move(optimized), .sinkOutputSchema = std::move(schema)};
    }

    /// Computes the text that an EXPLAIN answers with, by binding its statement and running the requested stages.
    [[nodiscard]] std::string explainOutput(const std::string& sql) const
    {
        auto binding = bindStatement(sql);
        auto* explainStatement = std::get_if<ExplainQueryStatement>(&binding);
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

PlannedTest SystestBinder::plan(const RewrittenTest& runnable)
{
    return impl->compile(runnable);
}

SystestBinder::~SystestBinder() = default;
}
