#pragma once

#include <string>
#include <memory>

#include <Core/DataTypes.hpp>

#include <CodeGen/CodeExpression.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
//#include <CodeGen/C_CodeGen/ForLoopStatement.hpp>

#include <Util/ErrorHandling.hpp>



namespace iotdb{

  ForLoopStatement::ForLoopStatement(const VariableDeclaration &var_decl, const ExpressionStatment &condition,
                                     const ExpressionStatment &advance, const std::vector<StatementPtr> &loop_body)
      : var_decl_(var_decl), condition_(condition.copy()), advance_(advance.copy()), loop_body_(loop_body) {}

  StatementType ForLoopStatement::getStamentType() const { return StatementType::FOR_LOOP_STMT; }
  const CodeExpressionPtr ForLoopStatement::getCode() const {
    std::stringstream code;
    code << "for(" << var_decl_.getCode() << ";" << condition_->getCode()->code_ << ";" << advance_->getCode()->code_
         << "){" << std::endl;
    for (const auto &stmt : loop_body_) {
      code << stmt->getCode()->code_ << ";" << std::endl;
    }
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
  }

  void ForLoopStatement::addStatement(StatementPtr stmt) {
    if (stmt)
      loop_body_.push_back(stmt);
  }


}
