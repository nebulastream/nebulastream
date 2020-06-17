
#include <DataTypes/DataTypeFactory.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/GeneratableValueType.hpp>
#include <utility>

namespace NES {

ConstantExpressionStatement::~ConstantExpressionStatement() {}

StatementType ConstantExpressionStatement::getStamentType() const {
    return CONSTANT_VALUE_EXPR_STMT;
}

const CodeExpressionPtr ConstantExpressionStatement::getCode() const {
    return constantValue->getCodeExpression();
}

const ExpressionStatmentPtr ConstantExpressionStatement::copy() const {
    auto vel = std::make_shared<ConstantExpressionStatement>(*this);
    vel->constantValue = constantValue;
    return vel;
}

ConstantExpressionStatement::ConstantExpressionStatement(GeneratableValueTypePtr val) : constantValue(std::move(val)) {}

}// namespace NES