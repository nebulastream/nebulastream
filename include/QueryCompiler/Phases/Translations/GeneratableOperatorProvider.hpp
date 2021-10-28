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
#ifndef NES_INCLUDE_QUERY_COMPILER_PHASES_TRANSLATIONS_GENERATABLE_OPERATOR_PROVIDER_HPP_
#define NES_INCLUDE_QUERY_COMPILER_PHASES_TRANSLATIONS_GENERATABLE_OPERATOR_PROVIDER_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {
/**
 * @brief This is a general interface, which provides the functionality to replace a logical
 * operator with corresponding physical operators.
 */
class GeneratableOperatorProvider {
  public:
    /**
     * @brief Replaces this node with physical operators that express the same semantics.
     * @param queryPlan the current query plan.
     * @param operatorNode the operator that should be replaced.
     */
    virtual void lower(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode) = 0;
};
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_PHASES_TRANSLATIONS_GENERATABLE_OPERATOR_PROVIDER_HPP_
