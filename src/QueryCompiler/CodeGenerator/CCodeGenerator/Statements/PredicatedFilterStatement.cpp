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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/PredicatedFilterStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <Util/Logger.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation {

ExpressionStatementPtr PredicatedFilterStatement::generatePredicateEvaluationCode(const ExpressionStatement& predicateExpression,
                                                                                  const VariableDeclaration& tuplePassesFilter,
                                                                                  bool tuplePassesPredicateIsDeclared) {
    auto predicateEvaluation = std::make_shared<CompoundStatement>();
    if (!tuplePassesPredicateIsDeclared) {
        // we need to declare the boolean tuplePassesFilter, then assign the evaluation of the predicate
        return VarDecl(tuplePassesFilter).assign(predicateExpression.copy()).copy();
    } else {
        // tuplePassesFilter has been previously declared and assigned. we rewrite it with the "AND" of this next predicate's evaluation
        return VarRef(tuplePassesFilter).assign((VarRef(tuplePassesFilter) & predicateExpression).copy()).copy();
    }
}

PredicatedFilterStatement::PredicatedFilterStatement(const ExpressionStatement& predicateExpression,
                                                     const VariableDeclaration& tuplePassesFilter,
                                                     bool tuplePassesPredicateIsDeclared)
    : predicateEvaluation(
        generatePredicateEvaluationCode(predicateExpression, tuplePassesFilter, tuplePassesPredicateIsDeclared)),
      predicatedCode(std::make_shared<CompoundStatement>()) {}

PredicatedFilterStatement::PredicatedFilterStatement(const ExpressionStatement& predicateExpression,
                                                     const VariableDeclaration& tuplePassesFilter,
                                                     bool tuplePassesPredicateIsDeclared,
                                                     const Statement& predicatedCode)
    : predicateEvaluation(
        generatePredicateEvaluationCode(predicateExpression, tuplePassesFilter, tuplePassesPredicateIsDeclared)),
      predicatedCode(std::make_shared<CompoundStatement>()) {
    this->predicatedCode->addStatement(predicatedCode.createCopy());
}

PredicatedFilterStatement::PredicatedFilterStatement(ExpressionStatementPtr predicateExpression,
                                                     const VariableDeclarationPtr tuplePassesFilter,
                                                     bool tuplePassesPredicateIsDeclared,
                                                     const StatementPtr predicatedCode)
    : predicateEvaluation(
        generatePredicateEvaluationCode(*predicateExpression, *tuplePassesFilter, tuplePassesPredicateIsDeclared)),
      predicatedCode(std::make_shared<CompoundStatement>()) {
    this->predicatedCode->addStatement(predicatedCode->createCopy());
}

StatementType PredicatedFilterStatement::getStamentType() const { return PREDICATED_FILTER_STMT; }
CodeExpressionPtr PredicatedFilterStatement::getCode() const {

    auto predicateEvaluationGenerated = predicateEvaluation->getCode()->code_;
    auto predicatedCodeGenerated = predicatedCode->getCode()->code_;
    NES_ASSERT(!predicateEvaluationGenerated.empty(),
               "PredicatedFilterstatement: predicateEvaluation did not generate valid code.");
    NES_ASSERT(!predicatedCodeGenerated.empty(), "PredicatedFilterstatement: predicatedCode did not generate valid code.");

    std::stringstream code;
    // evaluate the predicate (based on input buffer field) first - so that the field isn't overwritten with a result value in the buffer reuse optimization:
    code << predicateEvaluationGenerated << ";" << std::endl;
    // in a following emit operator the tuple is written to the result buffer as if not predicated.
    // but the index of the result buffer write location is only increased if the filter is passed.
    // thus only tuples that pass the filter will persist in the result buffer:
    code << predicatedCodeGenerated << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}
StatementPtr PredicatedFilterStatement::createCopy() const { return std::make_shared<PredicatedFilterStatement>(*this); }

CompoundStatementPtr PredicatedFilterStatement::getCompoundStatement() { return predicatedCode; }
}// namespace NES::QueryCompilation
