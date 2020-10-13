#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <utility>

namespace NES {

MinAggregationDescriptor::MinAggregationDescriptor(NES::AttributeFieldPtr field) : WindowAggregationDescriptor(std::move(field)) {}
MinAggregationDescriptor::MinAggregationDescriptor(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregationDescriptor(std::move(field), std::move(asField)) {}

WindowAggregationPtr MinAggregationDescriptor::create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr MinAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(keyField));
}

void MinAggregationDescriptor::compileLiftCombine(CompoundStatementPtr currentCode,
                             BinaryOperatorStatement expressionStatement,
                             StructDeclaration inputStruct,
                             BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto ifStatement = IF(
        expressionStatement > inputRef.accessRef(VarRefStatement(varDeclInput)),
        assign(expressionStatement, inputRef.accessRef(VarRefStatement(varDeclInput))));
    currentCode->addStatement(ifStatement.createCopy());
}
WindowAggregationDescriptor::Type MinAggregationDescriptor::getType() {
    return Min;
}
}// namespace NES