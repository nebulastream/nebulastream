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
#include <any>
#include <expected>
#include <SQLQueryParser/StatementBinder.hpp>
#include <ErrorHandling.hpp>


#include <StatementHandler.hpp>


namespace NES
{

SourceStatementHandler::SourceStatementHandler(const std::shared_ptr<Catalogs::Source::SourceCatalog>& source_catalog)
    : sourceCatalog(source_catalog)
{
}

std::expected<CreateLogicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const CreateLogicalSourceStatement& statement) noexcept
{
    if (const auto created = sourceCatalog->addLogicalSource(statement.name, statement.schema))
    {
        return CreateLogicalSourceStatementResult{created.value()};
    }
    return std::unexpected{SourceAlreadyExists(statement.name)};
}

std::expected<CreatePhysicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const CreatePhysicalSourceStatement& statement) noexcept
{
    if (const auto created = sourceCatalog->addPhysicalSource(
            statement.attachedTo, statement.sourceType, statement.workerId, statement.sourceConfig, statement.parserConfig))
    {
        return CreatePhysicalSourceStatementResult{created.value()};
    }
    return std::unexpected{InvalidConfigParameter("Invalid configuration: {}", statement)};
}

std::expected<ShowLogicalSourcesStatementResult, Exception>
SourceStatementHandler::operator()(const ShowLogicalSourcesStatement& statement) noexcept
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
SourceStatementHandler::operator()(const ShowPhysicalSourcesStatement& statement) noexcept
{
    if (statement.id and not statement.logicalSource)
    {
        if (const auto foundSource = sourceCatalog->getPhysicalSource(*statement.id))
        {
            return ShowPhysicalSourcesStatementResult{std::vector{*foundSource}};
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (not statement.id and statement.logicalSource)
    {
        if (const auto foundSources = sourceCatalog->getPhysicalSources(*statement.logicalSource))
        {
            return ShowPhysicalSourcesStatementResult{*foundSources | std::ranges::to<std::vector>()};
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    if (statement.logicalSource and statement.id)
    {
        if (const auto foundSources = sourceCatalog->getPhysicalSources(*statement.logicalSource))
        {
            return ShowPhysicalSourcesStatementResult{
                *foundSources
                | std::views::filter([statement](const auto& source) { return source.getPhysicalSourceId() == *statement.id; })
                | std::ranges::to<std::vector>()};
        }
        return ShowPhysicalSourcesStatementResult{{}};
    }
    return ShowPhysicalSourcesStatementResult{
        sourceCatalog->getAllSources() | std::views::transform([](auto& pair) { return pair.second; }) | std::views::join
        | std::ranges::to<std::vector>()};
}

std::expected<DropLogicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(const DropLogicalSourceStatement& statement) noexcept
{
    if (sourceCatalog->removeLogicalSource(statement.source))
    {
        return DropLogicalSourceStatementResult{statement.source};
    }
    return std::unexpected{UnregisteredSource(statement.source.getLogicalSourceName())};
}

std::expected<DropPhysicalSourceStatementResult, Exception>
SourceStatementHandler::operator()(DropPhysicalSourceStatement statement) noexcept
{
    if (sourceCatalog->removePhysicalSource(statement.descriptor))
    {
        return DropPhysicalSourceStatementResult{statement.descriptor};
    }
    return std::unexpected{UnregisteredSource("Unknown physical source: {}", statement.descriptor)};
}

QueryStatementHandler::QueryStatementHandler(const std::shared_ptr<CLI::Nebuli>& nebuli, const std::shared_ptr<CLI::Optimizer>& optimizer)
    : nebuli(nebuli), optimizer(optimizer)
{
}

std::expected<DropQueryStatementResult, Exception> QueryStatementHandler::operator()(DropQueryStatement statement)
{
    nebuli->stopQuery(statement.id);
    return DropQueryStatementResult{statement.id};
}

std::expected<QueryStatementResult, Exception> QueryStatementHandler::operator()(std::shared_ptr<QueryPlan> statement)
{
    const auto optimizedPlan = optimizer->optimize(statement);
    const auto id = nebuli->registerQuery(optimizedPlan);
    nebuli->startQuery(id);
    return QueryStatementResult{id};
}

}
