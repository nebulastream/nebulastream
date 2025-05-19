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
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <variant>
#include <AntlrSQLParser.h>
#include <Identifiers/Identifiers.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>

namespace NES::Binder
{

/// The source management statements are directly executed by the binder as we currently do not need to handle them differently between the frontends.
/// Should we require this in the future, we can change these structs to some intermediate representation with which the frontends have to go to the source catalog with.
struct CreateLogicalSourceStatement
{
    LogicalSource created;
};

struct CreatePhysicalSourceStatement
{
    Sources::SourceDescriptor created;
};

struct DropLogicalSourceStatement
{
    LogicalSource dropped;
};

struct DropPhysicalSourceStatement
{
    Sources::SourceDescriptor dropped;
};

struct DropQueryStatement
{
    QueryId id;
    explicit DropQueryStatement(const QueryId id) : id(id) { }
};

using Statement = std::variant<
    CreateLogicalSourceStatement,
    CreatePhysicalSourceStatement,
    DropLogicalSourceStatement,
    DropPhysicalSourceStatement,
    DropQueryStatement,
    std::shared_ptr<QueryPlan>>;

using BindingResult = std::expected<Statement, Exception>;


class StatementBinder
{
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;
    std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)> queryBinder;
    BindingResult bind(AntlrSQLParser::StatementContext* statementAST);

public:
    explicit StatementBinder(
        const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog,
        const std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept;

    BindingResult parseAndBind(std::string_view statementString);
};
}
