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

struct DropLogicalSourceStatementResult
{
    LogicalSource dropped;
};

struct DropPhysicalSourceStatementResult
{
    Sources::SourceDescriptor dropped;
};

struct DropQueryStatementResult
{
    QueryId id;
};

struct StartQueryStatementResult
{
    QueryId id;
};


using StatementResult = std::variant<
        CreateLogicalSourceStatementResult,
        CreatePhysicalSourceStatementResult,
        DropLogicalSourceStatementResult,
        DropPhysicalSourceStatementResult,
        DropQueryStatementResult>;

class StatementHandler
{
public:
    virtual ~StatementHandler() = default;
    virtual std::expected<StatementResult, Exception> execute(Binder::Statement statement) noexcept = 0;
};


class DefaultStatementHandler final : public StatementHandler
{
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;
    CLI::Nebuli nebuli;
    CLI::Optimizer optimizer;

public:
    DefaultStatementHandler(
            const std::shared_ptr<Catalogs::Source::SourceCatalog>& source_catalog,
            const CLI::Nebuli& nebuli,
            const CLI::Optimizer& optimizer)
            : sourceCatalog(source_catalog), nebuli(nebuli), optimizer(optimizer)
    {
    }

    std::expected<CreateLogicalSourceStatementResult, Exception> operator()(const Binder::CreateLogicalSourceStatement& statement);
    std::expected<CreatePhysicalSourceStatementResult, Exception> operator()(const Binder::CreatePhysicalSourceStatement& statement);
    std::expected<DropLogicalSourceStatementResult, Exception> operator()(const Binder::DropLogicalSourceStatement& statement);
    std::expected<DropPhysicalSourceStatementResult, Exception> operator()(Binder::DropPhysicalSourceStatement statement);
    std::expected<DropQueryStatementResult, Exception> operator()(Binder::DropQueryStatement statement);
    std::expected<StartQueryStatementResult, Exception> operator()(std::shared_ptr<QueryPlan> statement);

    std::expected<StatementResult, Exception> execute(Binder::Statement statement) noexcept override;
};
}