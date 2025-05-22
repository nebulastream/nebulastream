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
#include "SQLQueryParser/StatementBinder.hpp"


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

template <typename Handler, typename... Statements>
concept StatementHandlerFor = (requires(Handler handler_instance, Statements statement_instance) {
    { handler_instance(statement_instance) };
} && ...);

class SourceStatementHandler final
{
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;
public:
    explicit SourceStatementHandler(const std::shared_ptr<Catalogs::Source::SourceCatalog>& source_catalog);
    std::expected<CreateLogicalSourceStatementResult, Exception> operator()(const CreateLogicalSourceStatement& statement);
    std::expected<CreatePhysicalSourceStatementResult, Exception> operator()(const CreatePhysicalSourceStatement& statement);
    std::expected<DropLogicalSourceStatementResult, Exception> operator()(const DropLogicalSourceStatement& statement);
    std::expected<DropPhysicalSourceStatementResult, Exception> operator()(DropPhysicalSourceStatement statement);
};

class QueryStatementHandler final
{
    std::shared_ptr<CLI::Nebuli> nebuli;
    std::shared_ptr<CLI::Optimizer> optimizer;
public:
    explicit QueryStatementHandler(const std::shared_ptr<CLI::Nebuli>& nebuli, const std::shared_ptr<CLI::Optimizer>& optimizer);
    std::expected<StartQueryStatementResult, Exception> operator()(QueryStatement statement);
    std::expected<DropQueryStatementResult, Exception> operator()(DropQueryStatement statement);
};
}