
#include <QueryCompiler/CodeExpression.hpp>

namespace NES {

CodeExpression::CodeExpression(const std::string& code) : code_(code) {}

const CodeExpressionPtr combine(const CodeExpressionPtr lhs, const CodeExpressionPtr rhs) {
    return std::make_shared<CodeExpression>(lhs->code_ + rhs->code_);
}

}// namespace NES
