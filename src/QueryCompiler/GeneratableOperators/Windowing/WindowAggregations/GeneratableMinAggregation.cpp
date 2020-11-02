#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableMinAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <utility>
namespace NES{

GeneratableMinAggregation::GeneratableMinAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor): GeneratableWindowAggregation(std::move(aggregationDescriptor)) {}

GeneratableWindowAggregationPtr GeneratableMinAggregation::create(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor) {
    return std::make_shared<GeneratableMinAggregation>(aggregationDescriptor);
}

void GeneratableMinAggregation::compileLiftCombine(CompoundStatementPtr currentCode,
                                                   BinaryOperatorStatement partialRef,
                                                   StructDeclaration inputStruct,
                                                   BinaryOperatorStatement inputRef) {
    auto varDeclInput = inputStruct.getVariableDeclaration(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());
    auto ifStatement = IF(
        partialRef > inputRef.accessRef(VarRefStatement(varDeclInput)),
        assign(partialRef, inputRef.accessRef(VarRefStatement(varDeclInput))));
    currentCode->addStatement(ifStatement.createCopy());
}
}