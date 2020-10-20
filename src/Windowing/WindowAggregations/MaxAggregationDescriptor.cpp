#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <utility>

namespace NES {

MaxAggregationDescriptor::MaxAggregationDescriptor(FieldAccessExpressionNodePtr field) : WindowAggregationDescriptor(std::move(field)) {}

MaxAggregationDescriptor::MaxAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField) : WindowAggregationDescriptor(std::move(field), std::move(asField)) {}

WindowAggregationPtr MaxAggregationDescriptor::create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField) {
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr MaxAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(fieldAccess));
}

void MaxAggregationDescriptor::compileLiftCombine(CompoundStatementPtr currentCode,
                                                  BinaryOperatorStatement partialRef,
                                                  StructDeclaration inputStruct,
                                                  BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->as<FieldAccessExpressionNode>()->getFieldName());
    auto ifStatement = IF(
        partialRef < inputRef.accessRef(VarRefStatement(varDeclInput)),
        assign(partialRef, inputRef.accessRef(VarRefStatement(varDeclInput))));
    currentCode->addStatement(ifStatement.createCopy());
}
WindowAggregationDescriptor::Type MaxAggregationDescriptor::getType() {
    return Max;
}
void MaxAggregationDescriptor::inferStamp(SchemaPtr schema) {
    // We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!onField->getStamp()->isNumeric()) {
        NES_FATAL_ERROR("MaxAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }
    asField->setStamp(onField->getStamp());
}
WindowAggregationPtr MaxAggregationDescriptor::copy() {
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}
}// namespace NES