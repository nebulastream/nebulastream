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

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableWindowAggregation::GeneratableWindowAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor)
    : aggregationDescriptor(std::move(aggregationDescriptor)) {
    auto resultField = this->aggregationDescriptor->as()->as_if<FieldAccessExpressionNode>()->getFieldName();
    auto partialStamp = this->aggregationDescriptor->getPartialAggregateStamp();
    if (!partialStamp->isUndefined()) {
        auto partialAggregateDeclaration =
            VariableDeclaration::create(this->aggregationDescriptor->getPartialAggregateStamp(), resultField);
        this->partialAggregate = std::make_shared<VariableDeclaration>(partialAggregateDeclaration);
    }
}

Windowing::WindowAggregationDescriptorPtr GeneratableWindowAggregation::getAggregationDescriptor() {
    return aggregationDescriptor;
}

NES::QueryCompilation::VariableDeclarationPtr GeneratableWindowAggregation::getPartialAggregate() {
    return partialAggregate;
}

}// namespace NES::QueryCompilation::GeneratableOperators