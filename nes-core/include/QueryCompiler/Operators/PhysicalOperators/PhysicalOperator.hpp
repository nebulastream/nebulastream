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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_PHYSICAL_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_PHYSICAL_OPERATOR_HPP_

#include <Operators/OperatorNode.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief General abstraction for all physical operators.
 * A physical operator represents the concrete realization of a logical operator.
 * It can be a direct mapping, e.g., a LogicalFilterOperator maps to a PhysicalFilterOperator.
 * Other logical operators can also result in multiple physical operators,
 * e.g., CentralWindowOperator results in a SlicePreAggregation and a WindowSinkOperator.
 * This mapping is called lowering and is defined in a PhysicalOperatorProvider.
 */
class PhysicalOperator : public virtual OperatorNode {
  protected:
    explicit PhysicalOperator(OperatorId id);

  public:
    ~PhysicalOperator() noexcept = default;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_INCLUDE_QUERY_COMPILER_OPERATORS_PHYSICAL_OPERATORS_PHYSICAL_OPERATOR_HPP_
