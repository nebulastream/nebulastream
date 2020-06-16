
#include <DataTypes/DataTypeFactory.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/GeneratableValueType.hpp>

namespace NES {

ConstantExpressionStatement::~ConstantExpressionStatement() {}

StatementType ConstantExpressionStatement::getStamentType() const {
    return CONSTANT_VALUE_EXPR_STMT;
}

const CodeExpressionPtr ConstantExpressionStatement::getCode() const {
    return constantValue->generateCode();
}

const ExpressionStatmentPtr ConstantExpressionStatement::copy() const {
    return std::make_shared<ConstantExpressionStatement>(*this);
}

ConstantExpressionStatement::ConstantExpressionStatement(GeneratableValueTypePtr val) : constantValue(val) {}

}// namespace NES