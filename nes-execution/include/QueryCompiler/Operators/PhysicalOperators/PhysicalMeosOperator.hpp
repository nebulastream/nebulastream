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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMEOSTERNARYOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMEOSTERNARYOPERATOR_HPP_
#include <Operators/AbstractOperators/Arity/TernaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical Ternary operator combines the PhysicalOperator and TernaryOperator interfaces.
 * A physical Ternary operator has exactly two children operators.
 */
class PhysicalMeosOperator : public PhysicalOperator, public TernaryOperator {
  protected:
    PhysicalMeosOperator(OperatorId id,
                         StatisticId statisticId,
                         SchemaPtr leftSchema,
                         SchemaPtr middleSchema,
                         SchemaPtr rightSchema,
                         SchemaPtr outputSchema);

  public:
    /**
     * @brief returns the string representation of the class
     * @return the string representation of the class
     */
    std::string toString() const override;
    const std::string& getFunction() const;
    const std::vector<ExpressionNodePtr>& getInputFields() const;
    const std::vector<ExpressionNodePtr>& getOutputFields() const;

  protected:
    const std::string function;
    const std::vector<ExpressionNodePtr> inputFields;
    const std::vector<ExpressionNodePtr> outputFields;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMEOSTERNARYOPERATOR_HPP_
