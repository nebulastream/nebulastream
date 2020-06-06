
#include <QueryCompiler/CCodeGenerator/CompoundStatement.hpp>

#include <API/Types/DataTypes.hpp>

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
    //code << "{" << std::endl;
    for (const auto& stmt : statements) {
        code << stmt->getCode()->code_ << ";" << std::endl;
    }
    //code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}

void CompoundStatement::addStatement(StatementPtr stmt) {
    if (stmt)
        statements.push_back(stmt);
}

CompoundStatement::~CompoundStatement() {
}

}// namespace NES
