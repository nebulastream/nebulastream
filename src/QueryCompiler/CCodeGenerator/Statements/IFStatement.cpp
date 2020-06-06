

#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>

namespace NES {

IFStatement::~IFStatement() {}

IFStatement::IFStatement(const Statement& cond_expr) : conditionalExpression(cond_expr.createCopy()),
                                                       trueCaseStatement(new CompoundStatement()) {
}
IFStatement::IFStatement(const Statement& cond_expr, const Statement& cond_true_stmt)
    : conditionalExpression(cond_expr.createCopy()), trueCaseStatement(new CompoundStatement()) {
    trueCaseStatement->addStatement(cond_true_stmt.createCopy());
}

StatementType IFStatement::getStamentType() const { return IF_STMT; }
const CodeExpressionPtr IFStatement::getCode() const {
    std::stringstream code;
    code << "if(" << conditionalExpression->getCode()->code_ << "){" << std::endl;
    code << trueCaseStatement->getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}
const StatementPtr IFStatement::createCopy() const {
return std::make_shared<IFStatement>(*this);
}

const CompoundStatementPtr IFStatement::getCompoundStatement() {
    return trueCaseStatement;
}


}// namespace NES
