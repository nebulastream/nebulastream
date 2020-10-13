#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <utility>

namespace NES {

MaxAggregationDescriptor::MaxAggregationDescriptor(NES::AttributeFieldPtr field) : WindowAggregationDescriptor(std::move(field)) {}

MaxAggregationDescriptor::MaxAggregationDescriptor(AttributeFieldPtr field, AttributeFieldPtr asField) : WindowAggregationDescriptor(std::move(field), std::move(asField)) {}

WindowAggregationPtr MaxAggregationDescriptor::create(NES::AttributeFieldPtr onField, NES::AttributeFieldPtr asField) {
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr MaxAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    auto keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(keyField));
}

void MaxAggregationDescriptor::compileLiftCombine(CompoundStatementPtr currentCode,
                             BinaryOperatorStatement partialRef,
                             StructDeclaration inputStruct,
                             BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->name);
    auto ifStatement = IF(
        partialRef < inputRef.accessRef(VarRefStatement(varDeclInput)),
        assign(partialRef, inputRef.accessRef(VarRefStatement(varDeclInput))));
    currentCode->addStatement(ifStatement.createCopy());
}
WindowAggregationDescriptor::Type MaxAggregationDescriptor::getType() {
    return Max;
}
}// namespace NES