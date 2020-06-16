

#pragma once

#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class FunctionBuilder {
  private:
    std::string name;
    DataTypePtr returnType;
    std::vector<VariableDeclaration> parameters;
    std::vector<VariableDeclaration> variable_declarations;
    std::vector<StatementPtr> statements;
    FunctionBuilder(const std::string& function_name);

  public:
    static FunctionBuilder create(const std::string& function_name);

    FunctionBuilder& returns(DataTypePtr returnType_);
    FunctionBuilder& addParameter(VariableDeclaration var_decl);
    FunctionBuilder& addStatement(StatementPtr statement);
    FunctionBuilder& addVariableDeclaration(VariableDeclaration var_decl);
    FunctionDeclaration build();
};

}// namespace NES
