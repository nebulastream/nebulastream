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
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <experimental/propagate_const>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>

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
    QueryId id;
};

struct ShowQueriesStatementResult
{
    std::unordered_map<QueryId, LocalQueryStatus> queries;
};

struct DropQueryStatementResult
{
    QueryId id;
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

class QueryStatementHandler final : public StatementHandler<QueryStatementHandler>
{
    SharedPtr<QueryManager> queryManager;
    SharedPtr<const LegacyOptimizer> optimizer;

public:
    explicit QueryStatementHandler(SharedPtr<QueryManager> queryManager, SharedPtr<const LegacyOptimizer> optimizer);
    std::expected<QueryStatementResult, Exception> operator()(const QueryStatement& statement);
    std::expected<ShowQueriesStatementResult, Exception> operator()(const ShowQueriesStatement& statement);
    std::expected<DropQueryStatementResult, Exception> operator()(const DropQueryStatement& statement);

    [[nodiscard]] std::vector<QueryId> getRunningQueries() const;
};

}

FMT_OSTREAM(NES::CreateLogicalSourceStatementResult);
FMT_OSTREAM(NES::CreatePhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropLogicalSourceStatementResult);
FMT_OSTREAM(NES::DropPhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropQueryStatementResult);
FMT_OSTREAM(NES::QueryStatementResult);
