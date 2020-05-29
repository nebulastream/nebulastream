
#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

#include <API/Types/DataTypes.hpp>

namespace NES {

CompoundStatement::CompoundStatement()
    : statements_() {
}

StatementType CompoundStatement::getStamentType() const { return StatementType::COMPOUND_STMT; }

const CodeExpressionPtr CompoundStatement::getCode() const {
    std::stringstream code;
    //code << "{" << std::endl;
    for (const auto& stmt : statements_) {
        code << stmt->getCode()->code_ << ";" << std::endl;
    }
    //code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}

void CompoundStatement::addStatement(StatementPtr stmt) {
    if (stmt)
        statements_.push_back(stmt);
}

CompoundStatement::~CompoundStatement() {
}

}// namespace NES
