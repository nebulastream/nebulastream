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

#include <iostream>
#include <memory>
#include <string>

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/MultiOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>

namespace NES::QueryCompilation {
std::string toString(const MultiOperatorType& type) {
    const char* const names[] = {"CREATE_TENSOR_OP"};
    return std::string(names[type]);
}

CodeExpressionPtr toCodeExpression(const MultiOperatorType& type) {
    //TODO: figure out what to put here
    constexpr char const* const names[] = {
        "createTensor",
    };
    return std::make_shared<CodeExpression>(names[type]);
}

MultiOperatorStatement::MultiOperatorStatement(const std::vector<ExpressionStatement> expressions,
                                               MultiOperatorType const& op,
                                               BracketMode bracket_mode)
    : bracket_mode_(bracket_mode), _op(op) {
    for (auto& exp : expressions) {
        this->expressions.push_back(exp.copy());
    }
}

MultiOperatorStatement::MultiOperatorStatement(const std::vector<ExpressionStatementPtr> expressions,
                                               MultiOperatorType const& op,
                                               BracketMode bracket_mode)
    : bracket_mode_(bracket_mode), _op(op) {
    for (auto& exp : expressions) {
        this->expressions.push_back(exp);
    }
}

StatementType MultiOperatorStatement::getStamentType() const { return MULTI_OP_STMT; }

//todo: work on main portion!!!
//I need:
//- tensor information from query input aka from the expression node (shape, component type, tensortype)
//- creating a new tensor in the schema with generatableTensorValueType
CodeExpressionPtr MultiOperatorStatement::getCode() const {
    /*CodeExpressionPtr code;
    if (ARRAY_REFERENCE_OP == op_) {
        code = combine(lhs_->getCode(), std::make_shared<CodeExpression>("["));
        code = combine(code, rhs_->getCode());
        code = combine(code, std::make_shared<CodeExpression>("]"));
    } else if (POWER_OP == op_ || MODULO_OP == op_) {
        code = combine(toCodeExpression(op_), std::make_shared<CodeExpression>("("));// -> "pow(" or "fmod("
        code = combine(code, lhs_->getCode());
        code = combine(code, std::make_shared<CodeExpression>(","));
        code = combine(code, rhs_->getCode());
        code = combine(code, std::make_shared<CodeExpression>(")"));
    } else {
        code = combine(lhs_->getCode(), toCodeExpression(op_));
        code = combine(code, rhs_->getCode());
    }

    for (auto exp : expressions) {
    }
*/
    for (auto exp : expressions) {
        std::cout << "Current expression is: " << exp->getCode()->code_ << std::endl;
    }
    std::cout << "Current operator: " << std::endl;
    std::cout << _op << std::endl;
    std::string ret;
    if (bracket_mode_ == BRACKETS) {
        ret = std::string("(") + "code" + std::string(")");
    } else {
        ret = "code";
    }
    return std::make_shared<CodeExpression>(ret);
}

ExpressionStatementPtr MultiOperatorStatement::copy() const { return std::make_shared<MultiOperatorStatement>(*this); }

/** \brief small utility operator overloads to make code generation simpler and */

/*MultiOperatorStatement assign(const ExpressionStatement& lhs, const ExpressionStatement& rhs) {
    return MultiOperatorStatement(lhs, ASSIGNMENT_OP, rhs);
}*/

}// namespace NES::QueryCompilation
