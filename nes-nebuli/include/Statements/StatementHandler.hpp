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

#pragma once

#include <concepts>
#include <expected>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
/// Define the types and names of the output columns for each result type
struct CreateLogicalSourceStatementResult
{
    LogicalSource created;
};

struct CreatePhysicalSourceStatementResult
{
    SourceDescriptor created;
};

struct CreateSinkStatementResult
{
    SinkDescriptor created;
};

struct ShowLogicalSourcesStatementResult
{
    std::vector<LogicalSource> sources;
};

struct ShowPhysicalSourcesStatementResult
{
    std::vector<SourceDescriptor> sources;
};

struct ShowSinksStatementResult
{
    std::vector<SinkDescriptor> sinks;
};

struct DropLogicalSourceStatementResult
{
    LogicalSourceName dropped;
    Schema schema;
};

struct DropPhysicalSourceStatementResult
{
    SourceDescriptor dropped;
};

struct DropSinkStatementResult
{
    SinkDescriptor dropped;
};

struct QueryStatementResult
{
    LocalQueryId id;
};

struct ExplainQueryStatementResult
{
    std::string explainString;
};

struct WorkerStatusStatementResult
{
    WorkerStatus status;
};

struct ShowQueriesStatementResult
{
    std::unordered_map<LocalQueryId, LocalQueryStatus> queries;
};

struct DropQueryStatementResult
{
    LocalQueryId id;
};

using StatementResult = std::variant<
    CreateLogicalSourceStatementResult,
    CreatePhysicalSourceStatementResult,
    CreateSinkStatementResult,
    ShowLogicalSourcesStatementResult,
    ShowPhysicalSourcesStatementResult,
    ShowSinksStatementResult,
    DropLogicalSourceStatementResult,
    DropPhysicalSourceStatementResult,
    DropSinkStatementResult,
    QueryStatementResult,
    ShowQueriesStatementResult,
    ExplainQueryStatementResult,
    DropQueryStatementResult,
    WorkerStatusStatementResult>;

/// A bit of CRTP magic for nicer syntax when the object is in a shared ptr
template <typename HandlerImpl>
class StatementHandler
{
    StatementHandler() = default;

public:
    template <typename Statement>
    requires(std::invocable<HandlerImpl, const Statement&>)
    [[nodiscard]] auto apply(const Statement& statement) const noexcept -> decltype(std::declval<HandlerImpl>()(statement))
    {
        return static_cast<HandlerImpl*>(this)->operator()(statement);
    }

    template <typename Statement>
    requires(std::invocable<HandlerImpl, const Statement&>)
    auto apply(const Statement& statement) noexcept -> decltype(std::declval<HandlerImpl>()(statement))
    {
        return static_cast<HandlerImpl*>(this)->operator()(statement);
    }

    friend HandlerImpl;
};

struct DefaultHost
{
    std::string hostName;
};

struct RequireHostConfig
{
};

using HostPolicy = std::variant<RequireHostConfig, DefaultHost>;

class SourceStatementHandler final : public StatementHandler<SourceStatementHandler>
{
    std::shared_ptr<SourceCatalog> sourceCatalog;
    HostPolicy hostPolicy;

public:
    SourceStatementHandler(const std::shared_ptr<SourceCatalog>& sourceCatalog, HostPolicy hostPolicy);

    std::expected<CreateLogicalSourceStatementResult, Exception> operator()(const CreateLogicalSourceStatement& statement);
    std::expected<CreatePhysicalSourceStatementResult, Exception> operator()(const CreatePhysicalSourceStatement& statement);
    std::expected<ShowLogicalSourcesStatementResult, Exception> operator()(const ShowLogicalSourcesStatement& statement) const;
    std::expected<ShowPhysicalSourcesStatementResult, Exception> operator()(const ShowPhysicalSourcesStatement& statement) const;
    std::expected<DropLogicalSourceStatementResult, Exception> operator()(const DropLogicalSourceStatement& statement);
    std::expected<DropPhysicalSourceStatementResult, Exception> operator()(const DropPhysicalSourceStatement& statement);
};

class SinkStatementHandler final : public StatementHandler<SinkStatementHandler>
{
    std::shared_ptr<SinkCatalog> sinkCatalog;
    HostPolicy hostPolicy;

public:
    SinkStatementHandler(const std::shared_ptr<SinkCatalog>& sinkCatalog, HostPolicy hostPolicy);
    std::expected<CreateSinkStatementResult, Exception> operator()(const CreateSinkStatement& statement);
    std::expected<ShowSinksStatementResult, Exception> operator()(const ShowSinksStatement& statement) const;
    std::expected<DropSinkStatementResult, Exception> operator()(const DropSinkStatement& statement);
};

class QueryStatementHandler final : public StatementHandler<QueryStatementHandler>
{
    SharedPtr<QueryManager> queryManager;
    SharedPtr<const LegacyOptimizer> optimizer;

public:
    explicit QueryStatementHandler(SharedPtr<QueryManager> queryManager, SharedPtr<const LegacyOptimizer> optimizer);
    std::expected<QueryStatementResult, Exception> operator()(const QueryStatement& statement);
    std::expected<ExplainQueryStatementResult, Exception> operator()(const ExplainQueryStatement& statement);
    std::expected<ShowQueriesStatementResult, Exception> operator()(const ShowQueriesStatement& statement);
    std::expected<DropQueryStatementResult, Exception> operator()(const DropQueryStatement& statement);
};

class TopologyStatementHandler final : public StatementHandler<TopologyStatementHandler>
{
    SharedPtr<QueryManager> queryManager;

public:
    explicit TopologyStatementHandler(SharedPtr<QueryManager> queryManager);

    std::expected<WorkerStatusStatementResult, Exception> operator()(const WorkerStatusStatement& statement);
};

template <typename HandlerT>
bool tryCall(const Statement& statement, HandlerT& handler)
{
    return std::visit(
        [&]<typename StatementType>(const StatementType& typedStatement)
        {
            if constexpr (std::is_invocable_v<HandlerT&, const StatementType&>)
            {
                if (auto value = handler(typedStatement); !value)
                {
                    throw value.error();
                }
                return true;
            }
            return false;
        },
        statement);
}

template <typename HandlerT, typename... HandlerTs>
bool tryCall(const Statement& statement, HandlerT& handler, HandlerTs&... handlers)
{
    auto couldHandle = std::visit(
        [&]<typename StatementType>(const StatementType& typedStatement)
        {
            if constexpr (std::is_invocable_v<HandlerT&, const StatementType&>)
            {
                if (auto value = handler(typedStatement); !value)
                {
                    throw value.error();
                }
                return true;
            }
            return false;
        },
        statement);
    if (couldHandle)
    {
        return true;
    }
    return tryCall(statement, handlers...);
}

template <typename... HandlerT>
void handleStatements(const std::vector<Statement>& statements, HandlerT&... handler)
{
    for (const auto& statement : statements)
    {
        tryCall(statement, handler...);
    }
}

}

FMT_OSTREAM(NES::CreateLogicalSourceStatementResult);
FMT_OSTREAM(NES::CreatePhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropLogicalSourceStatementResult);
FMT_OSTREAM(NES::DropPhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropQueryStatementResult);
FMT_OSTREAM(NES::QueryStatementResult);
FMT_OSTREAM(NES::WorkerStatusStatementResult);
FMT_OSTREAM(NES::ExplainQueryStatementResult);
