
#include <DataTypes/ValueTypes/BasicValue.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableBasicValueType.hpp>
#include <utility>

namespace NES {

GeneratableBasicValueType::GeneratableBasicValueType(BasicValuePtr basicValue) : GeneratableValueType(), value(std::move(basicValue)) {}

CodeExpressionPtr GeneratableBasicValueType::getCodeExpression() {
    return std::make_shared<CodeExpression>(value->getValue());
}
}// namespace NES