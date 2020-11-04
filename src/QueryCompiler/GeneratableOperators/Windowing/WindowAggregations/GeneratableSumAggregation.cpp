#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableSumAggregation.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <utility>

namespace NES {

GeneratableSumAggregation::GeneratableSumAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor) : GeneratableWindowAggregation(std::move(aggregationDescriptor)) {}

GeneratableWindowAggregationPtr GeneratableSumAggregation::create(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor) {
    return std::make_shared<GeneratableSumAggregation>(aggregationDescriptor);
}

void GeneratableSumAggregation::compileLiftCombine(CompoundStatementPtr currentCode,
                                                   BinaryOperatorStatement partialRef,
                                                   StructDeclaration inputStruct,
                                                   BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());
    auto sum = partialRef + inputRef.accessRef(VarRefStatement(varDeclInput));
    auto updatedPartial = partialRef.assign(sum);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}
}// namespace NES