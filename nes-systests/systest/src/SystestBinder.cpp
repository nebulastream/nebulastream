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

#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <Config/Config.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Rewriter/Constants.hpp>
#include <Rewriter/SourceRewriting.hpp>
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

/// The schema of the file that a checksum sink writes, different from the schema of the rows.
/// Function-local static, because the schema resolves data types through a registry that plugins populate at startup.
const Schema<UnqualifiedUnboundField, Ordered>& getChecksumSchema()
{
    static const Schema<UnqualifiedUnboundField, Ordered> ChecksumSchema{std::vector{
        UnqualifiedUnboundField{Identifier::parse("COUNT"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        UnqualifiedUnboundField{Identifier::parse("CHECKSUM"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
    return ChecksumSchema;
}

Schema<UnqualifiedUnboundField, Ordered> getSinkOutputSchema(const DistributedLogicalPlan& plan)
{
    const auto sinkOperator = plan.getGlobalPlan().getRootOperators().at(0).tryGetAs<SinkLogicalOperator>();
    INVARIANT(sinkOperator.has_value(), "The optimized plan should have a sink operator");
    const auto& descriptor = sinkOperator.value()->getSinkDescriptor(); /// NOLINT(bugprone-unchecked-optional-access)
    INVARIANT(descriptor.has_value(), "The sink operator should have a sink descriptor");
    if (Sql::sameName(descriptor->getSinkType(), Sql::Checksum))
    {
        return getChecksumSchema();
    }
    return *get<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(descriptor->getSchema());
}

template <typename T>
void throwOnError(std::expected<T, Exception> result)
{
    if (not result.has_value())
    {
        throw std::move(result).error();
    }
}

/// A test case whose plan cannot be bound still becomes a query, so the whole run can report the failure.
template <typename Bind>
std::expected<PlanInfo, Exception> bindOrError(Bind&& bind)
{
    try
    {
        return std::forward<Bind>(bind)();
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
        : sourceCatalog(std::make_shared<SourceCatalog>())
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
        for (const auto& [host, data, capacity, downstream, workerConfig] : config.clusterConfig.workers)
        {
            workerCatalog->addWorker(host, data, capacity, downstream, workerConfig);
        }
    }

    /// Binds one rewritten test file part: its setup goes into the catalogs, and each test case becomes the statements to submit.
    /// The part key goes into each statement's distributed id, because the coordinator rejects two plans with one id and
    /// the parts of a file repeat its query numbers.
    [[nodiscard]] PlannedTest bind(const RunnableTestFile& runnable)
    {
        PlannedTest bound;
        /// The staged SQL is what reaches the catalogs, so a served source includes the endpoint that its server bound.
        for (auto setup : runnable.setupStatements)
        {
            stage(setup, bound.servers);
            submitToCatalogs(sqlOf(setup));
        }

        bound.testCases.reserve(runnable.testCases.size());
        for (const auto& [action] : runnable.testCases)
        {
            bound.testCases.push_back(std::visit(
                Overloaded{
                    [&](const RewrittenQuery& query) { return bindQuery(query, runnable.key); },
                    [&](const RewrittenDifferential& block) { return bindDifferential(block, runnable.key); },
                    [&](const RewrittenExplain& explain) { return bindExplain(explain); }},
                action));
        }
        return bound;
    }

private:
    /// Stages a setup statement's data before the statement reaches the catalogs.
    /// A served source gets the endpoint that its server bound merged into its statement.
    /// The server thread has to outlive the queries that read from it, so it is kept with the test file's queries.
    static void stage(SetupStatement& setup, std::vector<std::jthread>& sourceThreads)
    {
        std::visit(
            Overloaded{
                [](const PlainStatement&) {},
                [](const StatementWithInlineData& withInline) { writeInlineData(withInline.data); },
                [&](StatementWithServedData& withServed)
                {
                    auto [thread, options] = serve(std::move(withServed.data));
                    withServed.sql = addSourceOptions(withServed.sql, options);
                    sourceThreads.push_back(std::move(thread));
                }},
            setup);
    }

    void submitToCatalogs(const std::string& sql)
    {
        const auto binding = bindStatement(sql);
        std::visit(
            Overloaded{
                [&](const CreateLogicalSourceStatement& statement) { throwOnError(sourceHandler(statement)); },
                [&](const CreatePhysicalSourceStatement& statement) { throwOnError(sourceHandler(statement)); },
                [&](const CreateSinkStatement& statement) { throwOnError(sinkHandler(statement)); },
                [&](const CreateModelStatement& statement) { throwOnError(modelHandler(statement)); },
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
            throw InvalidQuerySyntax("failed to bind the statement \"{}\": {}", replaceAll(sql, "\n", " "), binding.error());
        }
        return std::move(binding).value();
    }

    /// Binds one query into the plan to submit.
    [[nodiscard]] std::vector<PlannedStatement> bindQuery(const RewrittenQuery& query, const std::string& partKey) const
    {
        auto plan = bindOrError([&] { return optimizeInto(query.sql, fmt::format("{}:{}", partKey, query.id.getRawValue())); });
        return {PlannedStatement{.plan = std::move(plan), .explained = std::nullopt}};
    }

    /// Binds both halves of a differential block, which run one after the other and are compared against each other.
    /// A half that does not bind makes the block fail, so the first failure is reported for the pair.
    [[nodiscard]] std::vector<PlannedStatement> bindDifferential(const RewrittenDifferential& block, const std::string& partKey) const
    {
        try
        {
            auto first = optimizeInto(block.firstSql, fmt::format("{}:{}", partKey, block.firstId.getRawValue()));
            auto second = optimizeInto(block.secondSql, fmt::format("{}:{}-differential", partKey, block.firstId.getRawValue()));
            std::vector<PlannedStatement> bound;
            bound.push_back(PlannedStatement{.plan = std::move(first), .explained = std::nullopt});
            bound.push_back(PlannedStatement{.plan = std::move(second), .explained = std::nullopt});
            return bound;
        }
        catch (Exception& exception)
        {
            return {PlannedStatement{.plan = std::unexpected{exception}, .explained = std::nullopt}};
        }
    }

    /// Computes an EXPLAIN here, not at run time, because only this component holds the optimizer that the
    /// OPTIMIZED and DISTRIBUTED stages need.
    /// It never reaches a worker and has no plan to run.
    [[nodiscard]] std::vector<PlannedStatement> bindExplain(const RewrittenExplain& explain) const
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
        auto schema = getSinkOutputSchema(optimized);
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

PlannedTest SystestBinder::bind(const RunnableTestFile& runnable)
{
    return impl->bind(runnable);
}

SystestBinder::~SystestBinder() = default;
}
