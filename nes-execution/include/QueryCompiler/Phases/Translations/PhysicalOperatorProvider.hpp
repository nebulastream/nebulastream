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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_PHYSICALOPERATORPROVIDER_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_PHYSICALOPERATORPROVIDER_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation {
/**
 * @brief This is a general interface, which provides the functionality to replace a logical
 * operator with corresponding physical operators.
 */
class PhysicalOperatorProvider {
  public:
    PhysicalOperatorProvider(QueryCompilerOptionsPtr options);
    /**
     * @brief Replaces this node with physical operators that express the same semantics.
     * @param decomposedQueryPlan the current decomposed query plan.
     * @param operatorNode the operator that should be replaced.
     */
    virtual void lower(DecomposedQueryPlanPtr decomposedQueryPlan, LogicalOperatorPtr operatorNode) = 0;

  protected:
    QueryCompilerOptionsPtr options;
};
}// namespace NES::QueryCompilation

#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_PHYSICALOPERATORPROVIDER_HPP_
