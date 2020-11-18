/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableFilterOperator.hpp>

namespace NES {

GeneratableFilterOperatorPtr GeneratableFilterOperator::create(FilterLogicalOperatorNodePtr filterLogicalOperator,
                                                               OperatorId id) {
    return std::make_shared<GeneratableFilterOperator>(GeneratableFilterOperator(filterLogicalOperator->getPredicate(), id));
}

GeneratableFilterOperator::GeneratableFilterOperator(const ExpressionNodePtr filterExpression, OperatorId id)
    : FilterLogicalOperatorNode(filterExpression, id) {}

void GeneratableFilterOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}

void GeneratableFilterOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // todo remove if code gen can handle expressions
    auto predicate = TranslateToLegacyPlanPhase::create()->transformExpression(getPredicate());
    codegen->generateCodeForFilter(std::dynamic_pointer_cast<Predicate>(predicate), context);
    getParents()[0]->as<GeneratableOperator>()->consume(codegen, context);
}

const std::string GeneratableFilterOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_FILTER(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES