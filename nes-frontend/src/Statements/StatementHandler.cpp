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

#include <Statements/StatementHandler.hpp>

#include <chrono>
#include <expected>
#include <filesystem>
#include <memory>
#include <ranges>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <Listeners/QueryLog.hpp>
#include <Phases/QueryOptimizer.hpp>
#include <Phases/SemanticAnalyzer.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Util/Pointers.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

SourceStatementHandler::SourceStatementHandler(const std::shared_ptr<SourceCatalog>& sourceCatalog) : sourceCatalog(sourceCatalog)
{
}

std::expected<CreateLogicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const CreateLogicalSourceStatement& statement)
{
    if (const auto created = sourceCatalog->addLogicalSource(statement.name, statement.schema))
    {
        return CreateLogicalSourceStatementResult{created.value()};
    }
    return std::unexpected{SourceAlreadyExists(statement.name)};
}

std::expected<CreatePhysicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const CreatePhysicalSourceStatement& statement)
{
    auto logicalSource = sourceCatalog->getLogicalSource(statement.attachedTo.getRawValue());
    if (!logicalSource)
    {
        return std::unexpected{UnknownSourceName(statement.attachedTo.getRawValue())};
    }

    if (const auto created
        = sourceCatalog->addPhysicalSource(*logicalSource, statement.sourceType, statement.sourceConfig, statement.parserConfig))
    {
        return CreatePhysicalSourceStatementResult{created.value()};
    }
    return std::unexpected{InvalidConfigParameter("Invalid configuration: {}", statement)};
}

std::expected<ShowLogicalSourcesStatementResult, Exception>
SourceStatementHandler::operator()(const ShowLogicalSourcesStatement& statement) const
{
    if (statement.name)
    {
        if (const auto foundSource = sourceCatalog->getLogicalSource(*statement.name))
        {
            return ShowLogicalSourcesStatementResult{std::vector{*foundSource}};
        }
        return ShowLogicalSourcesStatementResult{{}};
    }
    return ShowLogicalSourcesStatementResult{sourceCatalog->getAllLogicalSources() | std::ranges::to<std::vector>()};
}

std::expected<ShowPhysicalSourcesStatementResult, Exception>
SourceStatementHandler::operator()(const ShowPhysicalSourcesStatement& statement) const
{
    if (statement.id and not statement.logicalSource)
    {
        if (const auto foundSource = sourceCatalog->getPhysicalSource(PhysicalSourceId{statement.id.value()}))
        {
            return ShowPhysicalSourcesStatementResult{std::vector{*foundSource}};
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (not statement.id and statement.logicalSource)
    {
        if (const auto logicalSource = sourceCatalog->getLogicalSource(statement.logicalSource->getRawValue()))
        {
            if (const auto foundSources = sourceCatalog->getPhysicalSources(*logicalSource))
            {
                return ShowPhysicalSourcesStatementResult{*foundSources | std::ranges::to<std::vector>()};
            }
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (statement.logicalSource and statement.id)
    {
        if (const auto logicalSource = sourceCatalog->getLogicalSource(statement.logicalSource->getRawValue()))
        {
            if (const auto foundSources = sourceCatalog->getPhysicalSources(*logicalSource))
            {
                return ShowPhysicalSourcesStatementResult{
                    foundSources.value()
                    | std::views::filter([statement](const auto& source)
                                         { return source.getPhysicalSourceId() == PhysicalSourceId{statement.id.value()}; })
                    | std::ranges::to<std::vector>()};
            }
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    return ShowPhysicalSourcesStatementResult{
        sourceCatalog->getLogicalToPhysicalSourceMapping() | std::views::transform([](auto& pair) { return pair.second; })
        | std::views::join | std::ranges::to<std::vector>()};
}

std::expected<DropLogicalSourceStatementResult, Exception> SourceStatementHandler::operator()(const DropLogicalSourceStatement& statement)
{
    if (auto logical = sourceCatalog->getLogicalSource(statement.source.getRawValue()))
    {
        if (sourceCatalog->removeLogicalSource(*logical))
        {
            return DropLogicalSourceStatementResult{.dropped = statement.source, .schema = *logical->getSchema()};
        }
    }
    return std::unexpected{UnknownSourceName(statement.source.getRawValue())};
}

std::expected<DropPhysicalSourceStatementResult, Exception> SourceStatementHandler::operator()(const DropPhysicalSourceStatement& statement)
{
    if (sourceCatalog->removePhysicalSource(statement.descriptor))
    {
        return DropPhysicalSourceStatementResult{statement.descriptor};
    }
    return std::unexpected{UnknownSourceName("Unknown physical source: {}", statement.descriptor)};
}

SinkStatementHandler::SinkStatementHandler(const std::shared_ptr<SinkCatalog>& sinkCatalog) : sinkCatalog(sinkCatalog)
{
}

std::expected<CreateSinkStatementResult, Exception> SinkStatementHandler::operator()(const CreateSinkStatement& statement)
{
    if (const auto created = sinkCatalog->addSinkDescriptor(
            statement.name, statement.schema, statement.sinkType, statement.sinkConfig, statement.formatConfig))
    {
        return CreateSinkStatementResult{created.value()};
    }
    return std::unexpected{SinkAlreadyExists(statement.name)};
}

std::expected<ShowSinksStatementResult, Exception> SinkStatementHandler::operator()(const ShowSinksStatement& statement) const
{
    if (statement.name)
    {
        if (const auto foundSink = sinkCatalog->getSinkDescriptor(*statement.name))
        {
            return ShowSinksStatementResult{std::vector{*foundSink}};
        }
        return ShowSinksStatementResult{{}};
    }
    return ShowSinksStatementResult{sinkCatalog->getAllSinkDescriptors()};
}

std::expected<DropSinkStatementResult, Exception> SinkStatementHandler::operator()(const DropSinkStatement& statement)
{
    const auto sink = sinkCatalog->getSinkDescriptor(statement.name);
    if (not sink.has_value())
    {
        throw UnknownSinkName("Cannot remove unknown sink: {}", statement.name);
    }
    if (sinkCatalog->removeSinkDescriptor(sink.value()))
    {
        return DropSinkStatementResult{sink.value()};
    }
    return std::unexpected{UnknownSinkName(statement.name)};
}

ModelStatementHandler::ModelStatementHandler(std::shared_ptr<Inference::ModelCatalog> modelCatalog) : modelCatalog(std::move(modelCatalog))
{
}

namespace
{
ModelInfo toModelInfo(const Inference::RegisteredModel& reg)
{
    std::string inputSchema;
    for (const auto& [fieldName, dataType] : reg.inputs)
    {
        if (!inputSchema.empty())
        {
            inputSchema += ", ";
        }
        inputSchema += fmt::format("{}: {}", fieldName, magic_enum::enum_name(dataType->type));
    }

    std::string outputSchema;
    for (const auto& [fieldName, dataType] : reg.outputs)
    {
        if (!outputSchema.empty())
        {
            outputSchema += ", ";
        }
        outputSchema += fmt::format("{}: {}", fieldName, magic_enum::enum_name(dataType->type));
    }

    return ModelInfo{
        .name = reg.name,
        .path = reg.path.string(),
        .inputSchema = std::move(inputSchema),
        .outputSchema = std::move(outputSchema),
    };
}
}

std::expected<CreateModelStatementResult, Exception> ModelStatementHandler::operator()(const CreateModelStatement& statement)
{
    if (modelCatalog->hasModel(statement.name))
    {
        return std::unexpected{ModelAlreadyExists(statement.name)};
    }

    std::vector<std::pair<std::string, std::shared_ptr<DataType>>> inputs;
    inputs.reserve(statement.inputs.size());
    for (const auto& [fieldName, dataType] : statement.inputs)
    {
        inputs.emplace_back(fieldName, std::make_shared<DataType>(dataType));
    }

    std::vector<std::pair<std::string, std::shared_ptr<DataType>>> outputs;
    outputs.reserve(statement.outputs.size());
    for (const auto& [fieldName, dataType] : statement.outputs)
    {
        outputs.emplace_back(fieldName, std::make_shared<DataType>(dataType));
    }

    const Inference::RegisteredModel reg{
        .name = statement.name,
        .path = statement.path,
        .inputs = std::move(inputs),
        .outputs = std::move(outputs),
    };

    try
    {
        modelCatalog->registerModel(reg);
    }
    catch (const Exception& e)
    {
        return std::unexpected{e};
    }

    return toModelInfo(reg);
}

std::expected<ShowModelsStatementResult, Exception> ModelStatementHandler::operator()(const ShowModelsStatement&) const
{
    auto registeredModels = modelCatalog->getRegisteredModels();
    std::vector<ModelInfo> models;
    models.reserve(registeredModels.size());
    for (const auto& reg : registeredModels)
    {
        models.push_back(toModelInfo(reg));
    }
    return ShowModelsStatementResult{.models = std::move(models)};
}

std::expected<DropModelStatementResult, Exception> ModelStatementHandler::operator()(const DropModelStatement& statement)
{
    if (!modelCatalog->hasModel(statement.name))
    {
        return std::unexpected{UnknownModelName(statement.name)};
    }
    modelCatalog->removeModel(statement.name);
    return DropModelStatementResult{.name = statement.name};
}

QueryStatementHandler::QueryStatementHandler(
    SharedPtr<QueryManager> queryManager,
    SharedPtr<const SemanticAnalyzer> semanticAnalyser,
    SharedPtr<const QueryOptimizer> queryOptimizer)
    : queryManager(std::move(queryManager)), semanticAnalyser(std::move(semanticAnalyser)), queryOptimizer(std::move(queryOptimizer))
{
}

std::expected<DropQueryStatementResult, Exception> QueryStatementHandler::operator()(const DropQueryStatement& statement)
{
    return queryManager->stop(statement.id)
        .transform_error([](auto error) { return QueryStopFailed("Could not stop query: {}", error.what()); })
        .transform([&statement] { return DropQueryStatementResult{statement.id}; });
}

std::expected<ExplainQueryStatementResult, Exception> QueryStatementHandler::operator()(const ExplainQueryStatement& statement)
{
    CPPTRACE_TRY
    {
        std::stringstream explainMessage;
        fmt::println(explainMessage, "Query:\n{}", statement.plan.getOriginalSql());
        fmt::println(explainMessage, "Initial Logical Plan:\n{}", statement.plan);

        auto plan = semanticAnalyser->analyse(statement.plan);
        plan = queryOptimizer->optimize(plan);

        fmt::println(explainMessage, "Optimized Global Plan:\n{}", plan);

        return ExplainQueryStatementResult{explainMessage.str()};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<QueryStatementResult, Exception> QueryStatementHandler::operator()(const QueryStatement& statement)
{
    CPPTRACE_TRY
    {
        auto plan = semanticAnalyser->analyse(statement.plan);
        plan = queryOptimizer->optimize(plan);

        if (statement.id)
        {
            plan.setQueryId(QueryId(*statement.id));
        }
        const auto queryResult = queryManager->registerQuery(plan);
        return queryResult
            .and_then(
                [this](const auto& query)
                {
                    return queryManager->start(query)
                        .transform([&query] { return query; })
                        .transform_error([](auto error) { return QueryStartFailed("Could not start query: {}", error.what()); });
                })
            .transform([](auto query) { return QueryStatementResult{query}; });
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

TopologyStatementHandler::TopologyStatementHandler(SharedPtr<QueryManager> queryManager) : queryManager(std::move(queryManager))
{
}

std::expected<WorkerStatusStatementResult, Exception> TopologyStatementHandler::operator()(const WorkerStatusStatement&)
{
    return this->queryManager->workerStatus(std::chrono::system_clock::time_point{})
        .transform([](WorkerStatus status) { return WorkerStatusStatementResult{std::move(status)}; });
}

std::expected<ShowQueriesStatementResult, Exception> QueryStatementHandler::operator()(const ShowQueriesStatement& statement)
{
    if (not statement.id.has_value())
    {
        auto statusResults = queryManager->queries()
            | std::views::transform(
                                 [&](const auto& queryId) -> std::pair<QueryId, std::expected<LocalQueryStatus, Exception>>
                                 {
                                     auto statusResult = queryManager->status(queryId).transform_error(
                                         [](auto error) { return QueryStatusFailed("Could not fetch status for query: {}", error); });
                                     return {queryId, statusResult};
                                 })
            | std::ranges::to<std::vector>();

        auto failedStatusResults = statusResults
            | std::views::filter([](const auto& idAndStatusResult) { return !idAndStatusResult.second.has_value(); })
            | std::views::transform([](const auto& idAndStatusResult) -> std::pair<QueryId, Exception>
                                    { return {idAndStatusResult.first, idAndStatusResult.second.error()}; });

        auto goodQueryStatusResults = statusResults
            | std::views::filter([](const auto& idAndStatusResult) { return idAndStatusResult.second.has_value(); })
            | std::views::transform([](const auto& idAndStatusResult) -> std::pair<QueryId, LocalQueryStatus>
                                    { return {idAndStatusResult.first, idAndStatusResult.second.value()}; });
        if (!failedStatusResults.empty())
        {
            return std::unexpected(
                QueryStatusFailed("Could not retrieve query status for some queries: ", fmt::join(failedStatusResults, "\n")));
        }

        return ShowQueriesStatementResult{goodQueryStatusResults | std::ranges::to<std::unordered_map<QueryId, LocalQueryStatus>>()};
    }

    const auto statusOpt = queryManager->status(statement.id.value());
    if (statusOpt)
    {
        return ShowQueriesStatementResult{std::unordered_map<QueryId, LocalQueryStatus>{{statement.id.value(), statusOpt.value()}}};
    }
    return ShowQueriesStatementResult{std::unordered_map<QueryId, LocalQueryStatus>{
        {statement.id.value(),
         LocalQueryStatus{
             .queryId = statement.id.value(),
             .state = QueryState::Failed,
             .metrics = QueryMetrics{.start = {}, .running = {}, .stop = {}, .error = statusOpt.error()}}}}};
}
}
