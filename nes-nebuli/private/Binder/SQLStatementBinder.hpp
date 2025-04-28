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
#include <functional>
#include <variant>
#include <AntlrSQLParser.h>
#include <Plans/Query/QueryPlan.hpp>
#include <Sources/SourceCatalog.hpp>
#include <folly/stub/logging.h>


namespace NES::Binder
{


struct CreateSourceStatement
{
    std::string sourceName{};
    std::vector<std::pair<std::string, std::shared_ptr<DataType>>> columns;
    //to enable CREATE SOURCE AS add
    //std::optional<QueryPlan> sourceQuery;
};

struct DropSourceStatement
{
    std::variant<LogicalSource, Sources::SourceDescriptor> source;
};

struct DropQueryStatement
{
    QueryId id;
    explicit DropQueryStatement(const QueryId id) : id(id) { }
};

using Statement = std::variant<CreateSourceStatement, DropSourceStatement, DropQueryStatement, std::shared_ptr<QueryPlan>>;


class SQLStatementBinder
{
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;
    std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)> queryBinder;
    Statement bind(AntlrSQLParser::StatementContext* statementAST);

public:
    explicit SQLStatementBinder(
        const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog,
        const std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept;

    Statement parseAndBind(std::string_view statementString);
};
}
