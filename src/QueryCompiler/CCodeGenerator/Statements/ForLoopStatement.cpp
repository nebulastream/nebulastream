/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <QueryCompiler/CCodeGenerator/Statements/ForLoopStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace NES {

ForLoopStatement::ForLoopStatement(DeclarationPtr varDeclaration, ExpressionStatmentPtr condition, ExpressionStatmentPtr advance,
                                   const std::vector<StatementPtr>& loop_body)
    : varDeclaration(varDeclaration), condition(std::move(condition)), advance(std::move(advance)),
      body(new CompoundStatement()) {
    for (const auto& stmt : loop_body) {
        if (stmt)
            body->addStatement(stmt);
    }
}

const StatementPtr ForLoopStatement::createCopy() const { return std::make_shared<ForLoopStatement>(*this); }

const CompoundStatementPtr ForLoopStatement::getCompoundStatement() { return body; }

StatementType ForLoopStatement::getStamentType() const { return StatementType::FOR_LOOP_STMT; }

const CodeExpressionPtr ForLoopStatement::getCode() const {
    std::stringstream code;
    code << "for(" << varDeclaration->getCode() << ";" << condition->getCode()->code_ << ";" << advance->getCode()->code_ << "){"
         << std::endl;
    //    for (const auto& stmt : loop_body_) {
    //        code << stmt->getCode()->code_ << ";" << std::endl;
    //    }
    code << body->getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}

void ForLoopStatement::addStatement(StatementPtr stmt) {
    if (stmt)
        body->addStatement(stmt);
}

ForLoopStatement::~ForLoopStatement() {}

}// namespace NES
