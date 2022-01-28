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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_CEP_PHYSICALCEPITERATIONOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_CEP_PHYSICALCEPITERATIONOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Iteration CEPoperator.
 */
class PhysicalIterationCEPOperator : public PhysicalUnaryOperator {
  public:
    PhysicalIterationCEPOperator(OperatorId id,
                                 SchemaPtr inputSchema,
                                 SchemaPtr outputSchema,
                                 uint64_t minIterations,
                                 uint64_t maxIterations);
    static PhysicalOperatorPtr
    create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t minIterations, uint64_t maxIterations);
    static PhysicalOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, uint64_t minIterations, uint64_t maxIterations);
    std::string toString() const override;
    OperatorNodePtr copy() override;

    /**
   * @brief get the number of maxIterations.
   * @return uint64_t number of maxIterations
   */
    uint64_t getMaxIterations();

    /**
    * @brief get the number of minIterations.
    * @return uint64_t number of minIterations
    */
    uint64_t getMinIterations();

  private:
    /**
     * minIteration and maxIteration define the interval of event occurrences that satisfy the pattern condition
     */
    const uint64_t minIterations;
    const uint64_t maxIterations;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_CEP_PHYSICALCEPITERATIONOPERATOR_HPP_
