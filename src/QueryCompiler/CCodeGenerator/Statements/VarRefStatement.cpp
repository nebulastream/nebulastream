
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>

namespace NES {

StatementType VarRefStatement::getStamentType() const { return VAR_REF_STMT; }

const CodeExpressionPtr VarRefStatement::getCode() const { return varDeclaration->getIdentifier(); }

const ExpressionStatmentPtr VarRefStatement::copy() const { return std::make_shared<VarRefStatement>(*this); }

VarRefStatement::VarRefStatement(const VariableDeclaration& var_decl)
    : varDeclaration(std::dynamic_pointer_cast<VariableDeclaration>(var_decl.copy())) {
}

VarRefStatement::VarRefStatement(VariableDeclarationPtr varDeclaration)
: varDeclaration(std::move(varDeclaration)) {
}

VarRefStatement::~VarRefStatement() {}

}
