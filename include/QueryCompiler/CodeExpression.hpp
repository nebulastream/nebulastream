
#pragma once

#include <memory>
#include <string>

namespace NES {

class CodeExpression;
typedef std::shared_ptr<CodeExpression> CodeExpressionPtr;

class CodeExpression {
  public:
    CodeExpression(const std::string& code);
    // private:
    std::string code_;
};

const CodeExpressionPtr combine(const CodeExpressionPtr lhs, const CodeExpressionPtr rhs);

}// namespace NES
