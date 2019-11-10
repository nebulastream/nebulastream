
#include <memory>
#include <string>

#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/CodeExpression.hpp>

#include <Util/ErrorHandling.hpp>
#include "../../../include/CodeGen/DataTypes.hpp"

namespace iotdb {

Statement::~Statement() {}

const StatementPtr ExpressionStatment::createCopy() const{
  return this->copy();
}

ExpressionStatment::~ExpressionStatment() {}
ConstantExprStatement::~ConstantExprStatement() {}

TypeCastExprStatement::~TypeCastExprStatement() {}
VarRefStatement::~VarRefStatement() {}
IfStatement::~IfStatement() {}

ForLoopStatement::~ForLoopStatement() {}
UserDefinedDataType::~UserDefinedDataType() {}

FunctionCallStatement::~FunctionCallStatement() {}

const DataTypePtr createUserDefinedType(const StructDeclaration& decl)
{
    return std::make_shared<UserDefinedDataType>(decl);
}

} // namespace iotdb
