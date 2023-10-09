/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableAvgAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableAvgAggregation::GeneratableAvgAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor)
    : GeneratableWindowAggregation(std::move(aggregationDescriptor)) {}

GeneratableWindowAggregationPtr
GeneratableAvgAggregation::create(const Windowing::WindowAggregationDescriptorPtr& aggregationDescriptor) {
    return std::make_shared<GeneratableAvgAggregation>(aggregationDescriptor);
}

void GeneratableAvgAggregation::compileLiftCombine(CompoundStatementPtr currentCode,
                                                   BinaryOperatorStatement partialRef,
                                                   RecordHandlerPtr recordHandler) {

    auto fieldReference =
        recordHandler->getAttribute(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());

    auto addSumFunctionCall = FunctionCallStatement("addToSum");
    addSumFunctionCall.addParameter(*fieldReference);
    auto updateSumStatement = partialRef.accessRef(addSumFunctionCall);

    auto setCountFunctionCall = FunctionCallStatement("addToCount");
    auto updateCountStatement = partialRef.accessRef(setCountFunctionCall);

    currentCode->addStatement(updateSumStatement.copy());
    currentCode->addStatement(updateCountStatement.copy());
}
void GeneratableAvgAggregation::compileLift(CompoundStatementPtr currentCode,
                                            BinaryOperatorStatement partialValueRef,
                                            RecordHandlerPtr recordHandler) {

    auto fieldReference =
        recordHandler->getAttribute(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());
    auto resetFunctionCall = FunctionCallStatement("reset");
    auto resetStatement = partialValueRef.accessRef(resetFunctionCall);
    currentCode->addStatement(resetStatement.copy());
    auto addSumFunctionCall = FunctionCallStatement("addToSum");
    addSumFunctionCall.addParameter(*fieldReference);
    auto updateSumStatement = partialValueRef.accessRef(addSumFunctionCall);

    auto setCountFunctionCall = FunctionCallStatement("addToCount");
    auto updateCountStatement = partialValueRef.accessRef(setCountFunctionCall);

    currentCode->addStatement(updateSumStatement.copy());
    currentCode->addStatement(updateCountStatement.copy());
}

void GeneratableAvgAggregation::compileCombine(CompoundStatementPtr currentCode,
                                               VarRefStatement partialValueRef1,
                                               VarRefStatement partialValueRef2) {
    auto updatedPartial = partialValueRef1.accessPtr(VarRef(getPartialAggregate()))
                              .assign(partialValueRef1.accessPtr(VarRef(getPartialAggregate()))
                                      + partialValueRef2.accessPtr(VarRef(getPartialAggregate())));
    currentCode->addStatement(updatedPartial.copy());
}
VariableDeclarationPtr GeneratableAvgAggregation::getPartialAggregate() {
    auto tf = GeneratableTypesFactory();
    auto resultField = this->aggregationDescriptor->as()->as_if<FieldAccessExpressionNode>()->getFieldName();
    return std::make_shared<VariableDeclaration>(
        VariableDeclaration::create(tf.createAnonymusDataType("NES::Windowing::AVGDouble"), resultField));
}
ExpressionStatementPtr GeneratableAvgAggregation::lower(ExpressionStatementPtr partialValue) {
    auto tf = GeneratableTypesFactory();
    auto sum = VariableDeclaration::create(tf.createAnonymusDataType("auto"), "sum");
    auto count = VariableDeclaration::create(tf.createAnonymusDataType("auto"), "count");
    return (partialValue->accessRef(VarRef(sum)) / partialValue->accessRef(VarRef(count))).copy();
}

}// namespace NES::QueryCompilation::GeneratableOperators