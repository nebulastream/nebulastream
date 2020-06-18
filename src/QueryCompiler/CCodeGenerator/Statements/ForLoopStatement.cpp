
#include <QueryCompiler/CCodeGenerator/Statements/ForLoopStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <memory>
#include <string>
#include <sstream>
#include <utility>

namespace NES {

ForLoopStatement::ForLoopStatement(DeclarationPtr varDeclaration, ExpressionStatmentPtr condition,
                                   ExpressionStatmentPtr advance, const std::vector<StatementPtr>& loop_body)
    : varDeclaration(varDeclaration), condition(std::move(condition)), advance(std::move(advance)), body(new CompoundStatement()) {
    for (const auto& stmt : loop_body) {
        if (stmt)
            body->addStatement(stmt);
    }
}

const StatementPtr ForLoopStatement::createCopy() const {
    return std::make_shared<ForLoopStatement>(*this);
}

const CompoundStatementPtr ForLoopStatement::getCompoundStatement() {
    return body;
}

StatementType ForLoopStatement::getStamentType() const { return StatementType::FOR_LOOP_STMT; }

const CodeExpressionPtr ForLoopStatement::getCode() const {
    std::stringstream code;
    code << "for(" << varDeclaration->getCode() << ";" << condition->getCode()->code_ << ";" << advance->getCode()->code_
         << "){" << std::endl;
    //    for (const auto& stmt : loop_body_) {
    //        code << stmt->getCode()->code_ << ";" << std::endl;
    //    }
    code << body->getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}

void ForLoopStatement::addStatement(StatementPtr stmt) {
    if (stmt)
        body->addStatement(stmt);
}

ForLoopStatement::~ForLoopStatement() {}

}// namespace NES
