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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/PredicatedFilterStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <sstream>
#include <utility>
#include <Util/Logger.hpp>

namespace NES::QueryCompilation {

PredicatedFilterStatement::PredicatedFilterStatement(const Statement& condExpr, const VariableDeclaration& indexVariable)
: conditionalExpression(condExpr.createCopy()), indexVariable(indexVariable), predicatedCode(std::make_shared<CompoundStatement>()) {}

PredicatedFilterStatement::PredicatedFilterStatement(const Statement& condExpr, const VariableDeclaration& indexVariable, const Statement& predicatedCode)
: conditionalExpression(condExpr.createCopy()), indexVariable(indexVariable), predicatedCode(std::make_shared<CompoundStatement>()) {
    this->predicatedCode->addStatement(predicatedCode.createCopy());
}

PredicatedFilterStatement::PredicatedFilterStatement(StatementPtr condExpr, const VariableDeclarationPtr indexVariable, const StatementPtr predicatedCode)
: conditionalExpression(std::move(condExpr)), indexVariable(indexVariable), predicatedCode(std::make_shared<CompoundStatement>()) {
    this->predicatedCode->addStatement(predicatedCode->createCopy());
}

StatementType PredicatedFilterStatement::getStamentType() const { return PREDICATED_FILTER_STMT; }
CodeExpressionPtr PredicatedFilterStatement::getCode() const {
    auto conditionalExpressionGenerated = conditionalExpression->getCode()->code_;
    auto predicatedCodeGenerated = predicatedCode->getCode()->code_;
    auto indexVariableGenerated = indexVariable.getCode()->code_;
    NES_ASSERT(!conditionalExpressionGenerated.empty(),
               "PredicatedFilterstatement: conditionalExpression did not generate valid code.");
    NES_ASSERT(!predicatedCodeGenerated.empty(),
               "PredicatedFilterstatement: predicatedCodeGenerated did not generate valid code.");
    NES_ASSERT(!indexVariableGenerated.empty(),
               "PredicatedFilterstatement: indexVariableGenerated did not generate valid code.");

    std::stringstream code;
    // evaluate the predicate (based on input buffer field) first - so that the field isn't overwritten with a result value in the buffer reuse optimization
    code << "bool passesFilter = " << conditionalExpressionGenerated << ";" << std::endl;
    // execute lower code statements as if they were not predicated
    code << predicatedCodeGenerated << std::endl;
    // but then, only increase the index of the result buffer write location if condition is fulfilled
    code << indexVariableGenerated << " += passesFilter;" << std::endl;
    // thus only tuples that pass the condition will persist in the result buffer
    return std::make_shared<CodeExpression>(code.str());
}
StatementPtr PredicatedFilterStatement::createCopy() const { return std::make_shared<PredicatedFilterStatement>(*this); }

CompoundStatementPtr PredicatedFilterStatement::getCompoundStatement() { return predicatedCode; }
}// namespace NES::QueryCompilation
