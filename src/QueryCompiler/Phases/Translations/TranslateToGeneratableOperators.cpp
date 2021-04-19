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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Phases/Translations/GeneratableOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/TranslateToGeneratbaleOperatorsPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {

TranslateToGeneratableOperatorsPtr
TranslateToGeneratableOperators::TranslateToGeneratableOperators::create(GeneratableOperatorProviderPtr provider) {
    return std::make_shared<TranslateToGeneratableOperators>(provider);
}

TranslateToGeneratableOperators::TranslateToGeneratableOperators(GeneratableOperatorProviderPtr provider) : provider(provider) {}

OperatorPipelinePtr TranslateToGeneratableOperators::apply(OperatorPipelinePtr operatorPipeline) {
    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();
    for (auto node : nodes) {
        provider->lower(queryPlan, node->as<PhysicalOperators::PhysicalOperator>());
    }
    return operatorPipeline;
}

}// namespace QueryCompilation
}// namespace NES