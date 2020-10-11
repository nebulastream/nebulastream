#include <API/Expressions/Expressions.hpp>
#include <Windowing/WindowAggregations/WindowAggregation.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <iostream>

#include<QueryCompiler/CodeExpression.hpp>
namespace NES {

WindowAggregation::WindowAggregation(const NES::AttributeFieldPtr onField) : onField(onField),
                                                                             asField(const_cast<AttributeFieldPtr&>(onField)) {
}

WindowAggregation::WindowAggregation(const NES::AttributeFieldPtr onField, const NES::AttributeFieldPtr asField) : onField(onField), asField(asField) {
}

WindowAggregation& WindowAggregation::as(const NES::AttributeFieldPtr asField) {
    this->asField = asField;
    return *this;
}
}// namespace NES