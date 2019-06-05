
#include <memory>
#include <string>

#include <Core/DataTypes.hpp>

#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/CodeExpression.hpp>

#include <Util/ErrorHandling.hpp>

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

FunctionCallExpressionStatement::~FunctionCallExpressionStatement() {}

const DataTypePtr createUserDefinedType(const StructDeclaration& decl)
{
    return std::make_shared<UserDefinedDataType>(decl);
}

} // namespace iotdb
