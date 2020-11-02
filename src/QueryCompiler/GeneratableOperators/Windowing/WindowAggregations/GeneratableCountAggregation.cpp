#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableCountAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <utility>

namespace NES{

GeneratableCountAggregation::GeneratableCountAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor): GeneratableWindowAggregation(std::move(aggregationDescriptor)) {}

GeneratableWindowAggregationPtr GeneratableCountAggregation::create(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor) {
    return std::make_shared<GeneratableCountAggregation>(aggregationDescriptor);
}

void GeneratableCountAggregation::compileLiftCombine(CompoundStatementPtr currentCode,
                                                   BinaryOperatorStatement partialRef,
                                                   StructDeclaration inputStruct,
                                                   BinaryOperatorStatement) {
    auto varDeclInput = inputStruct.getVariableDeclaration(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());
    auto increment = ++partialRef;
    auto updatedPartial = partialRef.assign(increment);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}

}