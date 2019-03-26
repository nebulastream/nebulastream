

#pragma once

#include <string>
#include <memory>

#include <Core/DataTypes.hpp>

#include <CodeGen/CodeExpression.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>

#include <Util/ErrorHandling.hpp>

namespace iotdb{

class FunctionBuilder {
private:
  std::string name;
  DataTypePtr returnType;
  std::vector<VariableDeclaration> parameters;
  std::vector<VariableDeclaration> variable_declarations;
  std::vector<StatementPtr> statements;
  FunctionBuilder(const std::string &function_name);

public:
  static FunctionBuilder create(const std::string &function_name);

  FunctionBuilder &returns(DataTypePtr returnType_);
  FunctionBuilder &addParameter(VariableDeclaration var_decl);
  FunctionBuilder &addStatement(StatementPtr statement);
  FunctionBuilder &addVariableDeclaration(VariableDeclaration var_decl);
  FunctionDeclaration build();
};


}
