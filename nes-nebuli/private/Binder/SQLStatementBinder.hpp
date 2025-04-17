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
#include <Binder/SQLStatements.hpp>

#include <functional>
#include <variant>
#include <AntlrSQLParser.h>
#include <Plans/Query/QueryPlan.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>


namespace NES::Binder
{

class SQLStatementBinder
{
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;
    std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser*)> queryBinder;
    Statement bind(AntlrSQLParser::StatementContext* statementAST);

public:
    explicit SQLStatementBinder(
        const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog,
        const std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept;

    Statement parseAndBind(std::string_view statementString);
};
}
