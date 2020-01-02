
#include <CodeGen/CodeExpression.hpp>

namespace iotdb {

CodeExpression::CodeExpression(const std::string& code) : code_(code) {}

const CodeExpressionPtr combine(const CodeExpressionPtr& lhs, const CodeExpressionPtr& rhs)
{
    return std::make_shared<CodeExpression>(lhs->code_ + rhs->code_);
}

} // namespace iotdb
