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
#include <expected>
#include <memory>
#include <string>
#include <SQLQueryParser/StatementBinder.hpp>


#include <QueryManager/QueryManager.hpp>
#include <LegacyOptimizer.hpp>
#include "DataTypes/Schema.hpp"
#include "ErrorHandling.hpp"

#include <experimental/propagate_const>


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

struct QueryStatementResult
{
    QueryId id;
};

struct ShowQueriesStatementResult
{
    std::unordered_map<QueryId, QuerySummary> queries;
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
public:
    template <typename Statement>
    requires(requires(HandlerImpl handler, const Statement& statement) { handler(statement); })
    auto apply(const Statement& statement) noexcept -> decltype(std::declval<HandlerImpl>()(statement))
    {
        return static_cast<HandlerImpl*>(this)->operator()(statement);
    }

    // template <typename Statement>
    // requires (not requires(HandlerImpl handler, const Statement& statement) { handler(statement); })
    //          std::monostate apply(const Statement&) noexcept
    // {
    //     return std::monostate{};
    // }
};

class SourceStatementHandler final : public StatementHandler<SourceStatementHandler>
{
    std::shared_ptr<SourceCatalog> sourceCatalog;

public:
    explicit SourceStatementHandler(const std::shared_ptr<SourceCatalog>& sourceCatalog);
    std::expected<CreateLogicalSourceStatementResult, Exception> operator()(const CreateLogicalSourceStatement& statement) noexcept;
    std::expected<CreatePhysicalSourceStatementResult, Exception> operator()(const CreatePhysicalSourceStatement& statement) noexcept;
    std::expected<ShowLogicalSourcesStatementResult, Exception> operator()(const ShowLogicalSourcesStatement& statement) noexcept;
    std::expected<ShowPhysicalSourcesStatementResult, Exception> operator()(const ShowPhysicalSourcesStatement& statement) noexcept;
    std::expected<DropLogicalSourceStatementResult, Exception> operator()(const DropLogicalSourceStatement& statement) noexcept;
    std::expected<DropPhysicalSourceStatementResult, Exception> operator()(const DropPhysicalSourceStatement& statement) noexcept;
};

static_assert(requires(SourceStatementHandler sourceHandler, const DropPhysicalSourceStatement& createLogicalSourceStatement) {
    sourceHandler.apply(createLogicalSourceStatement);
});

class SinkStatementHandler final : public StatementHandler<SinkStatementHandler>
{
    std::shared_ptr<SinkCatalog> sinkCatalog;

public:
    explicit SinkStatementHandler(const std::shared_ptr<SinkCatalog>& sinkCatalog);
    std::expected<CreateSinkStatementResult, Exception> operator()(const CreateSinkStatement& statement) noexcept;
    std::expected<ShowSinksStatementResult, Exception> operator()(const ShowSinksStatement& statement) noexcept;
    std::expected<DropSinkStatementResult, Exception> operator()(const DropSinkStatement& statement) noexcept;
};

class QueryStatementHandler final : public StatementHandler<QueryStatementHandler>
{
    std::experimental::propagate_const<std::shared_ptr<QueryManager>> queryManager;
    std::shared_ptr<const CLI::LegacyOptimizer> optimizer;

public:
    explicit QueryStatementHandler(
        const std::shared_ptr<QueryManager>& queryManager, const std::shared_ptr<CLI::LegacyOptimizer>& optimizer);
    std::expected<QueryStatementResult, Exception> operator()(const QueryStatement& statement);
    std::expected<ShowQueriesStatementResult, Exception> operator()(const ShowQueriesStatement& statement);
    std::expected<DropQueryStatementResult, Exception> operator()(const DropQueryStatement& statement);
};

}
FMT_OSTREAM(NES::CreateLogicalSourceStatementResult);
FMT_OSTREAM(NES::CreatePhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropLogicalSourceStatementResult);
FMT_OSTREAM(NES::DropPhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropQueryStatementResult);
FMT_OSTREAM(NES::QueryStatementResult);
