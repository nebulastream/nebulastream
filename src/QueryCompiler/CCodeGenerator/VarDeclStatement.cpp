
#include <QueryCompiler/CCodeGenerator/VarDeclStatement.hpp>

namespace NES {

StatementType VarDeclStatement::getStamentType() const { return VAR_DEC_STMT; }

const CodeExpressionPtr VarDeclStatement::getCode() const {
    return std::make_shared<CodeExpression>(variableDeclaration->getCode());
}

const ExpressionStatmentPtr VarDeclStatement::copy() const { return std::make_shared<VarDeclStatement>(*this); }

VarDeclStatement::VarDeclStatement(const VariableDeclaration& var_decl)
    : variableDeclaration(std::dynamic_pointer_cast<VariableDeclaration>(var_decl.copy())) {
}


VarDeclStatement::~VarDeclStatement() {}

}
