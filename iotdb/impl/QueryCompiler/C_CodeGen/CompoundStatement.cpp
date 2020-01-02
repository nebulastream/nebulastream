
#include <memory>
#include <string>

#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/CodeExpression.hpp>

#include <Util/ErrorHandling.hpp>
#include <API/Types/DataTypes.hpp>

namespace iotdb {

  CompoundStatement::CompoundStatement()
      : statements_()
  {
  }

  StatementType CompoundStatement::getStamentType() const { return StatementType::COMPOUND_STMT; }

  const CodeExpressionPtr CompoundStatement::getCode() const
  {
      std::stringstream code;
      //code << "{" << std::endl;
      for (const auto& stmt : statements_) {
          code << stmt->getCode()->code_ << ";" << std::endl;
      }
      //code << "}" << std::endl;
      return std::make_shared<CodeExpression>(code.str());
  }

  void CompoundStatement::addStatement(StatementPtr stmt)
  {
      if (stmt)
          statements_.push_back(stmt);
  }

  CompoundStatement::~CompoundStatement(){

  }

} // namespace iotdb
