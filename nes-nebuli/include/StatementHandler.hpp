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
#include <mutex>
#include <unordered_map>
#include <variant>
#include <vector>

#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include "Util/Pointers.hpp"

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
    Sinks::SinkDescriptor created;
};

struct CreateWorkerStatementResult
{
    WorkerConfig created;
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
    std::vector<Sinks::SinkDescriptor> sinks;
};

struct ShowWorkersStatementResult
{
    std::vector<WorkerConfig> workers;
};

struct DropLogicalSourceStatementResult
{
    LogicalSource dropped;
};

struct DropPhysicalSourceStatementResult
{
    SourceDescriptor dropped;
};

struct DropSinkStatementResult
{
    Sinks::SinkDescriptor dropped;
};

struct DropWorkerStatementResult
{
    WorkerConfig dropped;
};

struct QueryStatementResult
{
    DistributedQueryId id;
};

struct ShowQueriesStatementResult
{
    std::unordered_map<DistributedQueryId, DistributedQueryStatus> queries;
};

struct DropQueryStatementResult
{
    DistributedQueryId id;
};

using StatementResult = std::variant<
    CreateLogicalSourceStatementResult,
    CreatePhysicalSourceStatementResult,
    CreateSinkStatementResult,
    CreateWorkerStatementResult,
    ShowLogicalSourcesStatementResult,
    ShowPhysicalSourcesStatementResult,
    ShowSinksStatementResult,
    ShowWorkersStatementResult,
    DropLogicalSourceStatementResult,
    DropPhysicalSourceStatementResult,
    DropSinkStatementResult,
    DropWorkerStatementResult,
    QueryStatementResult,
    ShowQueriesStatementResult,
    DropQueryStatementResult>;

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

class SourceStatementHandler final : public StatementHandler<SourceStatementHandler>
{
    std::shared_ptr<SourceCatalog> sourceCatalog;

public:
    explicit SourceStatementHandler(const std::shared_ptr<SourceCatalog>& sourceCatalog);
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

public:
    explicit SinkStatementHandler(const std::shared_ptr<SinkCatalog>& sinkCatalog);
    std::expected<CreateSinkStatementResult, Exception> operator()(const CreateSinkStatement& statement);
    std::expected<ShowSinksStatementResult, Exception> operator()(const ShowSinksStatement& statement) const;
    std::expected<DropSinkStatementResult, Exception> operator()(const DropSinkStatement& statement);
};

class WorkerStatementHandler final : public StatementHandler<WorkerStatementHandler>
{
    std::shared_ptr<WorkerCatalog> workerCatalog;

public:
    explicit WorkerStatementHandler(const std::shared_ptr<WorkerCatalog>& workerCatalog);
    std::expected<CreateWorkerStatementResult, Exception> operator()(const CreateWorkerStatement& statement);
    std::expected<ShowWorkersStatementResult, Exception> operator()(const ShowWorkersStatement& statement) const;
    std::expected<DropWorkerStatementResult, Exception> operator()(const DropWorkerStatement& statement);
};

class QueryStatementHandler final : public StatementHandler<QueryStatementHandler>
{
    mutable std::mutex mutex;
    SharedPtr<QueryManager> queryManager;
    SharedPtr<SourceCatalog> sourceCatalog;
    SharedPtr<SinkCatalog> sinkCatalog;
    SharedPtr<WorkerCatalog> workerCatalog;
    std::unordered_set<Query> runningQueries;

public:
    QueryStatementHandler(
        SharedPtr<QueryManager> queryManager,
        SharedPtr<SourceCatalog> sourceCatalog,
        SharedPtr<SinkCatalog> sinkCatalog,
        SharedPtr<WorkerCatalog> workerCatalog);
    std::expected<QueryStatementResult, Exception> operator()(const QueryStatement& statement);
    std::expected<ShowQueriesStatementResult, std::vector<Exception>> operator()(const ShowQueriesStatement& statement);
    std::expected<DropQueryStatementResult, std::vector<Exception>> operator()(const DropQueryStatement& statement);

    [[nodiscard]] auto getRunningQueries() const;
};

}

FMT_OSTREAM(NES::CreateLogicalSourceStatementResult);
FMT_OSTREAM(NES::CreatePhysicalSourceStatementResult);
FMT_OSTREAM(NES::CreateWorkerStatementResult);
FMT_OSTREAM(NES::DropLogicalSourceStatementResult);
FMT_OSTREAM(NES::DropPhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropWorkerStatementResult);
FMT_OSTREAM(NES::DropQueryStatementResult);
FMT_OSTREAM(NES::QueryStatementResult);
