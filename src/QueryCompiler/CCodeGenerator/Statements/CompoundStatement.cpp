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

#include <QueryCompiler/CCodeGenerator/Statements/CompoundStatement.hpp>

#include <sstream>

namespace NES {

CompoundStatement::CompoundStatement() : statements() {}

const StatementPtr CompoundStatement::createCopy() const { return std::make_shared<CompoundStatement>(*this); }

StatementType CompoundStatement::getStamentType() const { return StatementType::COMPOUND_STMT; }

const CodeExpressionPtr CompoundStatement::getCode() const {
    std::stringstream code;
    for (const auto& stmt : statements) {
        code << stmt->getCode()->code_ << ";" << std::endl;
    }
    return std::make_shared<CodeExpression>(code.str());
}

void CompoundStatement::addStatement(StatementPtr stmt) {
    if (stmt)
        statements.push_back(stmt);
}

CompoundStatement::~CompoundStatement() {}

}// namespace NES
