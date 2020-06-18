#include <QueryCompiler/CCodeGenerator/Statements/CompoundStatement.hpp>

#include <sstream>

namespace NES {

CompoundStatement::CompoundStatement()
    : statements() {
}

const StatementPtr CompoundStatement::createCopy() const {
    return std::make_shared<CompoundStatement>(*this);
}

StatementType CompoundStatement::getStamentType() const { return StatementType::COMPOUND_STMT; }

const CodeExpressionPtr CompoundStatement::getCode() const {
    std::stringstream code;
    for (const auto& stmt : statements) {
        code << stmt->getCode()->code_ << ";" << std::endl;
    }
    return std::make_shared<CodeExpression>(code.str());
}

void CompoundStatement::addStatement(StatementPtr stmt) {
    if (stmt)
        statements.push_back(stmt);
}

CompoundStatement::~CompoundStatement() {
}

}// namespace NES
