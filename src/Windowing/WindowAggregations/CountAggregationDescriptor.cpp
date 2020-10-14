#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <utility>

namespace NES {

CountAggregationDescriptor::CountAggregationDescriptor(FieldAccessExpressionNodePtr field) : WindowAggregationDescriptor(std::move(field)) {}
CountAggregationDescriptor::CountAggregationDescriptor(FieldAccessExpressionNodePtr field, FieldAccessExpressionNodePtr asField) : WindowAggregationDescriptor(std::move(field), std::move(asField)) {}

WindowAggregationPtr CountAggregationDescriptor::create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField) {
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr CountAggregationDescriptor::on() {
    auto countField =  FieldAccessExpressionNode::create(DataTypeFactory::createInt64(),"count");
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(countField->as<FieldAccessExpressionNode>()));
}

void CountAggregationDescriptor::compileLiftCombine(CompoundStatementPtr currentCode,
                               BinaryOperatorStatement partialRef,
                               StructDeclaration inputStruct,
                               BinaryOperatorStatement) {
    auto varDeclInput = inputStruct.getVariableDeclaration(this->onField->getFieldName());
    auto increment = ++partialRef;
    auto updatedPartial = partialRef.assign(increment);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}
WindowAggregationDescriptor::Type CountAggregationDescriptor::getType() {
    return Count;
}

void CountAggregationDescriptor::inferStamp(SchemaPtr) {
    // a count aggregation is always on an int 64
    asField->setStamp(DataTypeFactory::createInt64());
}
}// namespace NES