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


#include <string>
#include <string_view>

#include <tuple>
#include <Binder/SQLStatementBinder.hpp>
#include <sys/stat.h>
#include <DataTypes/DataTypeProvider.hpp>

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <ErrorHandling.hpp>

namespace NES::Binder
{
SQLStatementBinder::SQLStatementBinder(
    const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog,
    const std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept
    : sourceCatalog(sourceCatalog), queryBinder(queryPlanBinder)
{
}

Statement SQLStatementBinder::bind(AntlrSQLParser::StatementContext* statementAST)
{
    if (statementAST->query() != nullptr)
    {
        throw NotImplemented("Cannot parse query here yet");
    }
    if (auto* const createAST = statementAST->createStatement(); createAST != nullptr)
    {
        if (auto* const sourceDefAST = createAST->createDefinition()->createSourceDefinition(); sourceDefAST != nullptr)
        {
            auto statement = CreateSourceStatement{};
            auto& [sourceName, columns] = statement;

            sourceName = sourceDefAST->sourceName->getText();

            for (auto* const column : sourceDefAST->schemaDefinition())
            {
                if (auto dataType = DataTypeProvider::tryProvideDataType(column->type->getText()))
                {
                    columns.emplace_back(std::pair{column->IDENTIFIER()->getText(), *dataType});
                }
                else
                {
                    throw UnknownType(column->type->getText());
                }
            }
            return statement;
        }
    }
    if (auto* const dropAst = statementAST->dropStatement(); dropAst != nullptr)
    {
        if (const auto* const dropSourceAst = dropAst->dropSubject()->dropSource(); dropSourceAst != nullptr)
        {
            if (const auto sourceName = dropSourceAst->name->getText(); sourceCatalog->containsLogicalSource(sourceName))
            {
                return DropSourceStatement{sourceCatalog->getLogicalSource(sourceName)};
            }
            else
            {
                //TODO How can I retrieve physical sources from catalogs?
                throw UnknownSourceType(sourceName);
            }
        }
        else if (const auto* const dropQueryAst = dropAst->dropSubject()->dropQuery(); dropQueryAst != nullptr)
        {
            const auto id = QueryId{std::stoul(dropQueryAst->id->getText())};
            return DropQueryStatement{id};
        }
    }
    if (auto* const queryAst = statementAST->query(); queryAst != nullptr)
    {
        return queryBinder(queryAst);
    }

    throw InvalidStatement(statementAST->toString());
}
Statement SQLStatementBinder::parseAndBind(const std::string_view statementString)
{
    antlr4::ANTLRInputStream input(statementString.data(), statementString.length());
    AntlrSQLLexer lexer(&input);
    antlr4::CommonTokenStream tokens(&lexer);
    AntlrSQLParser parser(&tokens);
    /// Enable that antlr throws exeptions on parsing errors
    parser.setErrorHandler(std::make_shared<antlr4::BailErrorStrategy>());
    AntlrSQLParser::StatementContext* tree = parser.statement();
    return bind(tree);
}
}
