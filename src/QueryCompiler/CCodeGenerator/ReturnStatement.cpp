
#include <QueryCompiler/CCodeGenerator/ReturnStatement.hpp>

namespace NES {

ReturnStatement::ReturnStatement(VarRefStatement var_ref) : var_ref_(var_ref) {}

StatementType ReturnStatement::getStamentType() const {
    return RETURN_STMT;
}

const CodeExpressionPtr ReturnStatement::getCode() const {
    std::stringstream stmt;
    stmt << "return " << var_ref_.getCode()->code_ << ";";
    return std::make_shared<CodeExpression>(stmt.str());
}

const StatementPtr ReturnStatement::createCopy() const {
    return std::make_shared<ReturnStatement>(*this);
}

ReturnStatement::~ReturnStatement() {}

}// namespace NES
