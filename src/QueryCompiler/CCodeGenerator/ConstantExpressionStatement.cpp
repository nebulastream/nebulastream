
#include <QueryCompiler/CCodeGenerator/ConstantExpressionStatement.hpp>

namespace NES {

ConstantExpressionStatement::~ConstantExpressionStatement() {}

StatementType ConstantExpressionStatement::getStamentType() const {
    return CONSTANT_VALUE_EXPR_STMT;
}

const CodeExpressionPtr ConstantExpressionStatement::getCode() const {
    return constantValue->getCodeExpression();
}

const ExpressionStatmentPtr ConstantExpressionStatement::copy() const {
    return std::make_shared<ConstantExpressionStatement>(*this);
}

ConstantExpressionStatement::ConstantExpressionStatement(const ValueTypePtr& val) : constantValue(val) {}

ConstantExpressionStatement::ConstantExpressionStatement(const BasicType& type, const std::string& value) : constantValue(createBasicTypeValue(type, value)) {}

ConstantExpressionStatement::ConstantExpressionStatement(int32_t value) : ConstantExpressionStatement(INT32, std::to_string(value)) {}

}// namespace NES