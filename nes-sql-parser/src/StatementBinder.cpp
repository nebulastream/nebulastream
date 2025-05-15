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


#include <memory>
#include <string>
#include <string_view>

#include <tuple>

#include <sys/stat.h>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include "Sources/SourceDescriptor.hpp"

#include <ANTLRInputStream.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <ErrorHandling.hpp>
#include <Sources/SourceCatalog.hpp>
#include <SQLQueryParser/StatementBinder.hpp>

namespace NES::Binder
{
StatementBinder::StatementBinder(
    const std::shared_ptr<Catalogs::Source::SourceCatalog>& sourceCatalog,
    const std::function<std::shared_ptr<QueryPlan>(AntlrSQLParser::QueryContext*)>& queryPlanBinder) noexcept
    : sourceCatalog(sourceCatalog), queryBinder(queryPlanBinder)
{
}

Statement StatementBinder::bind(AntlrSQLParser::StatementContext* statementAST)
{
    if (statementAST->query() != nullptr)
    {
        return queryBinder(statementAST->query());
    }
    if (auto* const createAST = statementAST->createStatement(); createAST != nullptr)
    {
        if (auto* const logicalSourceDefAST = createAST->createDefinition()->createLogicalSourceDefinition(); logicalSourceDefAST != nullptr)
        {
            auto statement = CreateLogicalSourceStatement{};
            auto& [sourceName, columns] = statement;

            sourceName = logicalSourceDefAST->sourceName->getText();

            for (auto* const column : logicalSourceDefAST->schemaDefinition()->columnDefinition())
            {
                if (auto dataType = DataTypeProvider::tryProvideDataType(column->typeDefinition()->getText()))
                {
                    columns.emplace_back(std::pair{column->IDENTIFIER()->getText(), *dataType});
                }
                else
                {
                    throw UnknownDataType(column->typeDefinition()->getText());
                }
            }
            return statement;
        } if (auto* const physicalSourceDefAST = createAST->createDefinition()->createPhysicalSourceDefinition(); physicalSourceDefAST != nullptr)
        {
            WorkerId workerId = INITIAL<WorkerId>;
            int32_t localBuffers = Sources::SourceDescriptor::INVALID_BUFFERS_IN_LOCAL_POOL;
            for (auto *options = physicalSourceDefAST->options; const auto& option : options->namedConfigExpression())
            {
                /// TODO use identifer list logic from #764
                if (option->name->ident.size() != 2)
                {
                    throw InvalidConfigParameter("{}", option->name->getText());
                }
                if (option->name->ident.at(0)->identifier()->getText() == "SOURCE")
                {
                    if (option->name->ident.at(1)->identifier()->getText() == "LOCATION")
                    {
                        if (auto* const stringLocation = dynamic_cast<AntlrSQLParser::StringLiteralContext*>(option->constant()))
                        {
                            if (stringLocation->getText() != "LOCAL")
                            {
                                throw InvalidConfigParameter(
                                    "SOURCE.LOCATION must be either an integer or \'LOCAL\' but was {}", stringLocation->getText());
                            }
                        } else if (auto* const intLocation = dynamic_cast<AntlrSQLParser::IntegerLiteralContext*>(option->constant()))
                        {
                            int convertedID = std::stoi(intLocation->getText());
                            if (convertedID < 0)
                            {
                                throw InvalidConfigParameter("SOURCE.LOCATION cannot be a negative integer, but was {}", convertedID);
                            }
                            workerId = WorkerId{static_cast<unsigned long>(convertedID)};
                        }
                    } else if (option->name->ident.at(1)->identifier()->getText() == "POOL_BUFFERS")
                    {
                        if (auto* const poolBuffersLiteral = dynamic_cast<AntlrSQLParser::IntegerLiteralContext*>(option->constant()))
                        {
                            localBuffers = std::stoi(poolBuffersLiteral->getText());
                        } else
                        {
                            throw InvalidConfigParameter("SOURCE.POOL_BUFFERS must be an integer, but was {}", option->constant());
                        }
                    }
                }
            }
            const auto logicalSourceOpt = sourceCatalog->getLogicalSource(physicalSourceDefAST->logicalSource->getText());
            if (not logicalSourceOpt.has_value())
            {
                throw UnregisteredSource("{}", physicalSourceDefAST->logicalSource->getText());
            }
            return CreatePhysicalSourceStatement{logicalSourceOpt.value(), descriptorString};
        }
    }
    if (auto* dropAst = statementAST->dropStatement(); dropAst != nullptr)
    {
        if (AntlrSQLParser::DropSourceContext* dropSourceAst = dropAst->dropSubject()->dropSource(); dropSourceAst != nullptr)
        {
            if (const auto *const logicalSourceSubject = dropSourceAst->dropLogicalSourceSubject();
                logicalSourceSubject != nullptr && sourceCatalog->containsLogicalSource(logicalSourceSubject->name->getText()))
            {
                if (auto const logicalSource = sourceCatalog->getLogicalSource(logicalSourceSubject->name->getText());
                    logicalSource.has_value())
                {
                    return DropSourceStatement{sourceCatalog->getLogicalSource(logicalSourceSubject->name->getText()).value()};
                }
                else
                {
                    throw UnregisteredSource(logicalSourceSubject->name->getText());
                }
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
Statement StatementBinder::parseAndBind(const std::string_view statementString)
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
