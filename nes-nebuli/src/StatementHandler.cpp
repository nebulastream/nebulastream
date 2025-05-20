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
#include "SQLQueryParser/StatementBinder.hpp"
#include <ErrorHandling.hpp>


#include <StatementHandler.hpp>


namespace NES
{

std::expected<CreateLogicalSourceStatementResult, Exception>
DefaultStatementHandler::operator()(const Binder::CreateLogicalSourceStatement& statement) noexcept
{
    if (const auto created = sourceCatalog->addLogicalSource(statement.name, statement.schema))
    {
        return CreateLogicalSourceStatementResult{created.value()};
    }
    return std::unexpected{SourceAlreadyExists(statement.name)};
}

std::expected<CreatePhysicalSourceStatementResult, Exception>
DefaultStatementHandler::operator()(const Binder::CreatePhysicalSourceStatement& statement) noexcept
{
    if (const auto created = sourceCatalog->addPhysicalSource(
            statement.attachedTo, statement.sourceType, statement.workerId, statement.sourceConfig, statement.parserConfig))
    {
        return CreatePhysicalSourceStatementResult{created.value()};
    }
    return std::unexpected{InvalidConfigParameter("Invalid configuration: {}", statement)};
}

std::expected<DropLogicalSourceStatementResult, Exception>
DefaultStatementHandler::operator()(const Binder::DropLogicalSourceStatement& statement) noexcept
{
    if (sourceCatalog->removeLogicalSource(statement.source))
    {
        return DropLogicalSourceStatementResult{statement.source};
    }
    return std::unexpected{UnregisteredSource(statement.source.getLogicalSourceName())};
}

std::expected<DropPhysicalSourceStatementResult, Exception>
DefaultStatementHandler::operator()(Binder::DropPhysicalSourceStatement statement) noexcept
{
    if (sourceCatalog->removePhysicalSource(statement.descriptor))
    {
        return DropPhysicalSourceStatementResult{statement.descriptor};
    }
    return std::unexpected{UnregisteredSource("Unknown physical source: {}", statement.descriptor)};
}

std::expected<DropQueryStatementResult, Exception> DefaultStatementHandler::operator()(Binder::DropQueryStatement statement) noexcept
{
    nebuli.stopQuery(statement.id);
    return DropQueryStatementResult{statement.id};
}

std::expected<StartQueryStatementResult, Exception> DefaultStatementHandler::operator()(std::shared_ptr<QueryPlan> statement)
{
    const auto optimizedPlan = optimizer.optimize(statement);
    const auto id = nebuli.registerQuery(optimizedPlan);
    nebuli.startQuery(id);
    return StartQueryStatementResult{id};
}
std::expected<StatementResult, Exception> DefaultStatementHandler::execute(Binder::Statement statement)
{
    try
    {
        return std::visit(*this, statement);
    } catch (Exception& e)
    {
        return std::unexpected{e};
    }
}
}
