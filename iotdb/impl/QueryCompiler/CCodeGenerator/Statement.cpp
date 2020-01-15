
#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>

namespace NES {

Statement::~Statement() {}

const StatementPtr ExpressionStatment::createCopy() const {
  return this->copy();
}

ExpressionStatment::~ExpressionStatment() {}
ConstantExprStatement::~ConstantExprStatement() {}

TypeCastExprStatement::~TypeCastExprStatement() {}
VarRefStatement::~VarRefStatement() {}
VarDeclStatement::~VarDeclStatement() {}
IfStatement::~IfStatement() {}

ForLoopStatement::~ForLoopStatement() {}
UserDefinedDataType::~UserDefinedDataType() {}
AnnonymUserDefinedDataType::~AnnonymUserDefinedDataType() {}

FunctionCallStatement::~FunctionCallStatement() {}

const DataTypePtr createUserDefinedType(const StructDeclaration &decl) {
  return std::make_shared<UserDefinedDataType>(decl);
}

const DataTypePtr createAnnonymUserDefinedType(const std::string name) {
  return std::make_shared<AnnonymUserDefinedDataType>(name);
}

} // namespace NES
