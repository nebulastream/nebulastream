
#include <QueryCompiler/CCodeGenerator/Statements/FunctionCallStatement.hpp>
namespace NES {

FunctionCallStatement::~FunctionCallStatement() {}

StatementType FunctionCallStatement::getStamentType() const {
    return FUNC_CALL_STMT;
}

const CodeExpressionPtr FunctionCallStatement::getCode() const {

    u_int32_t i;

    CodeExpressionPtr code;
    code = combine(std::make_shared<CodeExpression>(functionName), std::make_shared<CodeExpression>("("));
    for (i = 0; i < expressions.size(); i++) {
        if (i != 0)
            code = combine(code, std::make_shared<CodeExpression>(", "));
        code = combine(code, expressions.at(i)->getCode());
    }
    code = combine(code, std::make_shared<CodeExpression>(")"));
    return code;
}

const ExpressionStatmentPtr FunctionCallStatement::copy() const {
    return std::make_shared<FunctionCallStatement>(*this);
}

void FunctionCallStatement::addParameter(const ExpressionStatment& expr) {
    expressions.push_back(expr.copy());
}

void FunctionCallStatement::addParameter(ExpressionStatmentPtr expr) {
    expressions.push_back(expr);
}

FunctionCallStatement::FunctionCallStatement(const std::string functionname) : functionName(functionname) {}

}// namespace NES
