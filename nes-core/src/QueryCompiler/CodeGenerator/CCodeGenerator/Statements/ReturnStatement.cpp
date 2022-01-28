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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <sstream>
#include <utility>
namespace NES::QueryCompilation {
ReturnStatement::ReturnStatement(StatementPtr statement) : statement(std::move(statement)) {}

StatementPtr ReturnStatement::create(const StatementPtr& statement) { return std::make_shared<ReturnStatement>(statement); }

StatementType ReturnStatement::getStamentType() const { return RETURN_STMT; }

CodeExpressionPtr ReturnStatement::getCode() const {
    std::stringstream stmt;
    stmt << "return " << statement->getCode()->code_ << ";";
    return std::make_shared<CodeExpression>(stmt.str());
}

StatementPtr ReturnStatement::createCopy() const { return std::make_shared<ReturnStatement>(*this); }

}// namespace NES::QueryCompilation
