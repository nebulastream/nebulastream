#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <utility>

namespace NES {

CountAggregationDescriptor::CountAggregationDescriptor(NES::AttributeFieldPtr field) : WindowAggregationDescriptor(std::move(field)) {}
CountAggregationDescriptor::CountAggregationDescriptor(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregationDescriptor(std::move(field), std::move(asField)) {}

WindowAggregationPtr CountAggregationDescriptor::create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr CountAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(keyField));
}

void CountAggregationDescriptor::compileLiftCombine(CompoundStatementPtr currentCode,
                               BinaryOperatorStatement partialRef,
                               StructDeclaration inputStruct,
                               BinaryOperatorStatement) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto increment = ++partialRef;
    auto updatedPartial = partialRef.assign(increment);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}
WindowAggregationDescriptor::Type CountAggregationDescriptor::getType() {
    return Count;
}
}// namespace NES