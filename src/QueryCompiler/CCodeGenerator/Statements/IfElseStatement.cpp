
#include <QueryCompiler/CCodeGenerator/Statements/IFElseStatement.hpp>

namespace NES {

IfElseStatement::IfElseStatement(const Statement& cond_true, const Statement& cond_false) {}

const CodeExpressionPtr IfElseStatement::getCode() const {
    return std::make_shared<CodeExpression>("");
}

StatementType IfElseStatement::getStamentType() const {
    return IF_STMT;
}

const StatementPtr IfElseStatement::createCopy() const {
    return std::make_shared<IfElseStatement>(*this);
}

IfElseStatement::~IfElseStatement() noexcept {}

}// namespace NES
