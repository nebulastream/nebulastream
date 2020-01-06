
#include <memory>
#include <string>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

#include <API/Types/DataTypes.hpp>

namespace iotdb {

ForLoopStatement::ForLoopStatement(const VariableDeclaration& var_decl, const ExpressionStatment& condition,
                                   const ExpressionStatment& advance, const std::vector<StatementPtr>& loop_body)
    : var_decl_(var_decl), condition_(condition.copy()), advance_(advance.copy()), loop_body_(new CompoundStatement())
{
  for(const auto& stmt : loop_body){
    if(stmt)
      loop_body_->addStatement(stmt);
  }
}

StatementType ForLoopStatement::getStamentType() const { return StatementType::FOR_LOOP_STMT; }
const CodeExpressionPtr ForLoopStatement::getCode() const
{
    std::stringstream code;
    code << "for(" << var_decl_.getCode() << ";" << condition_->getCode()->code_ << ";" << advance_->getCode()->code_
         << "){" << std::endl;
//    for (const auto& stmt : loop_body_) {
//        code << stmt->getCode()->code_ << ";" << std::endl;
//    }
    code << loop_body_->getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}

void ForLoopStatement::addStatement(StatementPtr stmt)
{
    if (stmt)
        loop_body_->addStatement(stmt);
}

} // namespace iotdb
