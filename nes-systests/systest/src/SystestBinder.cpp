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
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <Config/Config.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Rewriter/Constants.hpp>
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
#include <DistributedLogicalPlan.hpp>
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

}

struct SystestBinder::Impl
{
    explicit Impl(const SystestConfiguration& config)
        : sourceCatalog{std::make_shared<SourceCatalog>()}
        , sinkCatalog{std::make_shared<SinkCatalog>()}
        , modelCatalog{std::make_shared<ModelCatalog>()}
        , workerCatalog{std::make_shared<WorkerCatalog>()}
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

    /// Binds every partition in order, so a partition's setup is in the catalogs before its test cases bind.
    void bindAll(const std::span<const PartitionToBind> partitions)
    {
        bound.reserve(partitions.size());
        for (const auto& partition : partitions)
        {
            bound.push_back(bindOne(partition));
        }
    }

    /// One result per bound partition, in binding order.
    std::vector<BindResult> bound;

private:
    /// A rejected setup statement fails the partition. The error also covers an exception that is not ours, such as
    /// from parsing a number, so the partition is reported rather than the run ending.
    [[nodiscard]] BindResult bindOne(const PartitionToBind& partition)
    {
        try
        {
            for (const auto& sql : partition.setupSql)
            {
                writeToCatalogs(sql);
            }
        }
        catch (...)
        {
            return std::unexpected{wrapExternalException()};
        }

        BoundTestFile file;
        file.testCases.reserve(partition.testCases.size());
        for (const auto& [action] : partition.testCases)
        {
            try
            {
                file.testCases.push_back(std::visit(
                    Overloaded{
                        [&](const RewrittenQuery& query) { return bindQuery(query, partition.partitionKey); },
                        [&](const RewrittenDifferential& block) { return bindDifferential(block, partition.partitionKey); },
                        [&](const RewrittenExplain& explain) { return bindExplain(explain); }},
                    action));
            }
            catch (const Exception& exception)
            {
                file.testCases.push_back({BoundStatement{.plan = std::unexpected{exception}, .explained = std::nullopt}});
            }
        }
        return file;
    }

    void writeToCatalogs(const std::string& sql)
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

    [[nodiscard]] std::vector<BoundStatement> bindQuery(const RewrittenQuery& query, const std::string& partitionKey) const
    {
        auto plan = optimizeInto(query.sql, fmt::format("{}:{}", partitionKey, query.id.getRawValue()));
        return {BoundStatement{.plan = std::move(plan), .explained = std::nullopt}};
    }

    /// A failed half fails the block, so the pair reports the first failure.
    [[nodiscard]] std::vector<BoundStatement> bindDifferential(const RewrittenDifferential& block, const std::string& partitionKey) const
    {
        auto first = optimizeInto(block.firstSql, fmt::format("{}:{}", partitionKey, block.firstId.getRawValue()));
        auto second = optimizeInto(block.secondSql, fmt::format("{}:{}-differential", partitionKey, block.firstId.getRawValue()));
        return {
            BoundStatement{.plan = std::move(first), .explained = std::nullopt},
            BoundStatement{.plan = std::move(second), .explained = std::nullopt}};
    }

    /// Computes an EXPLAIN here, because only this component holds the optimizer that the OPTIMIZED and DISTRIBUTED stages need.
    [[nodiscard]] std::vector<BoundStatement> bindExplain(const RewrittenExplain& explain) const
    {
        return {BoundStatement{
            .plan = std::unexpected{TestException("an EXPLAIN is not executed and has no plan")}, .explained = runExplain(explain.sql)}};
    }

    /// The query id correlates the statement with its answer.
    [[nodiscard]] BoundPlan optimizeInto(const std::string& sql, const std::string& queryId) const
    {
        auto plan = AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(sql);
        plan.setQueryId(QueryId::createDistributed(DistributedQueryId{queryId}));
        auto optimized = queryOptimizer.optimize(plan);
        auto schema = getSinkOutputSchema(optimized);
        return BoundPlan{.plan = std::move(optimized), .sinkOutputSchema = std::move(schema)};
    }

    [[nodiscard]] std::string runExplain(const std::string& sql) const
    {
        const auto binding = bindStatement(sql);
        const auto* explainStatement = std::get_if<ExplainQueryStatement>(&binding);
        if (explainStatement == nullptr)
        {
            throw UnsupportedQuery("expected an EXPLAIN statement, but got: \"{}\"", replaceAll(sql, "\n", " "));
        }
        return computeExplainOutput(*explainStatement, queryOptimizer);
    }

    /// One catalog set for the whole run, which the rewriter's prefixing keeps collision-free.
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

SystestBinder::SystestBinder(const SystestConfiguration& config, const std::span<const PartitionToBind> partitions)
    : impl{std::make_unique<Impl>(config)}
{
    impl->bindAll(partitions);
}

const std::vector<BindResult>& SystestBinder::getBound() const&
{
    return impl->bound;
}

std::vector<BindResult> SystestBinder::getBound() &&
{
    return std::move(impl->bound);
}

SystestBinder::~SystestBinder() = default;

}
