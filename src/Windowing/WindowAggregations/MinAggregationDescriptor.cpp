#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <utility>

namespace NES::Windowing {

MinAggregationDescriptor::MinAggregationDescriptor(FieldAccessExpressionNodePtr field) : WindowAggregationDescriptor(std::move(field)) {}
MinAggregationDescriptor::MinAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField) : WindowAggregationDescriptor(std::move(field), std::move(asField)) {}

WindowAggregationPtr MinAggregationDescriptor::create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField) {
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr MinAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(fieldAccess));
}

void MinAggregationDescriptor::compileLiftCombine(CompoundStatementPtr currentCode,
                                                  BinaryOperatorStatement expressionStatement,
                                                  StructDeclaration inputStruct,
                                                  BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->as<FieldAccessExpressionNode>()->getFieldName());
    auto ifStatement = IF(
        expressionStatement > inputRef.accessRef(VarRefStatement(varDeclInput)),
        assign(expressionStatement, inputRef.accessRef(VarRefStatement(varDeclInput))));
    currentCode->addStatement(ifStatement.createCopy());
}
WindowAggregationDescriptor::Type MinAggregationDescriptor::getType() {
    return Min;
}

void MinAggregationDescriptor::inferStamp(SchemaPtr schema) {
    // We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!onField->getStamp()->isNumeric()) {
        NES_FATAL_ERROR("SumAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }
    asField->setStamp(onField->getStamp());
}
WindowAggregationPtr MinAggregationDescriptor::copy() {
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}

}// namespace NES