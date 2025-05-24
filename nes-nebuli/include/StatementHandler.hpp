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
#include <memory>
#include <SQLQueryParser/StatementBinder.hpp>


#include <NebuLI.hpp>


namespace NES
{
struct CreateLogicalSourceStatementResult
{
    LogicalSource created;
};

struct CreatePhysicalSourceStatementResult
{
    Sources::SourceDescriptor created;
};

struct ShowLogicalSourcesStatementResult
{
    std::vector<LogicalSource> sources;
};

struct ShowPhysicalSourcesStatementResult
{
    std::vector<Sources::SourceDescriptor> sources;
};

struct DropLogicalSourceStatementResult
{
    LogicalSource dropped;
};

struct DropPhysicalSourceStatementResult
{
    Sources::SourceDescriptor dropped;
};

struct QueryStatementResult
{
    QueryId id;
};

struct ShowQueriesStatementResult
{
    std::map<QueryId, QuerySummaryReply> queries;
};

struct DropQueryStatementResult
{
    QueryId id;
};

using StatementResult = std::variant<
    CreateLogicalSourceStatementResult,
    CreatePhysicalSourceStatementResult,
    ShowLogicalSourcesStatementResult,
    ShowPhysicalSourcesStatementResult,
    DropLogicalSourceStatementResult,
    DropPhysicalSourceStatementResult,
    QueryStatementResult,
    ShowQueriesStatementResult,
    DropQueryStatementResult>;


/// A bit of CRTP magic for nicer syntax when the object is in a shared ptr
template <typename HandlerImpl>
class StatementHandler
{
public:
    template <typename Statement>
    requires (requires(HandlerImpl handler, const Statement& statement) { handler(statement); })
    auto apply(const Statement& statement) noexcept -> decltype(std::declval<HandlerImpl>()(statement))
    {
        return static_cast<HandlerImpl*>(this)->operator()(statement);
    }

    template <typename Statement>
    requires (not requires(HandlerImpl handler, const Statement& statement) { handler(statement); })
             std::monostate apply(const Statement&) noexcept
    {
        return std::monostate{};
    }
};

class SourceStatementHandler final : public StatementHandler<SourceStatementHandler>
{
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;

public:
    explicit SourceStatementHandler(const std::shared_ptr<Catalogs::Source::SourceCatalog>& source_catalog);
    std::expected<CreateLogicalSourceStatementResult, Exception> operator()(const CreateLogicalSourceStatement& statement) noexcept;
    std::expected<CreatePhysicalSourceStatementResult, Exception> operator()(const CreatePhysicalSourceStatement& statement) noexcept;
    std::expected<ShowLogicalSourcesStatementResult, Exception> operator()(const ShowLogicalSourcesStatement& statement) noexcept;
    std::expected<ShowPhysicalSourcesStatementResult, Exception> operator()(const ShowPhysicalSourcesStatement& statement) noexcept;
    std::expected<DropLogicalSourceStatementResult, Exception> operator()(const DropLogicalSourceStatement& statement) noexcept;
    std::expected<DropPhysicalSourceStatementResult, Exception> operator()(DropPhysicalSourceStatement statement) noexcept;
};

class QueryStatementHandler final : public StatementHandler<QueryStatementHandler>
{
    std::shared_ptr<CLI::Nebuli> nebuli;
    std::shared_ptr<CLI::Optimizer> optimizer;

public:
    explicit QueryStatementHandler(const std::shared_ptr<CLI::Nebuli>& nebuli, const std::shared_ptr<CLI::Optimizer>& optimizer);
    std::expected<QueryStatementResult, Exception> operator()(QueryStatement statement);
    std::expected<ShowQueriesStatementResult, Exception> operator()(ShowQueriesStatement statement);
    std::expected<DropQueryStatementResult, Exception> operator()(DropQueryStatement statement);
};

}
FMT_OSTREAM(NES::CreateLogicalSourceStatementResult);
FMT_OSTREAM(NES::CreatePhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropLogicalSourceStatementResult);
FMT_OSTREAM(NES::DropPhysicalSourceStatementResult);
FMT_OSTREAM(NES::DropQueryStatementResult);
FMT_OSTREAM(NES::QueryStatementResult);
