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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALINFERMODELOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALINFERMODELOPERATOR_HPP_

#include <Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical InferModel operator.
 */
class PhysicalInferModelOperator : public PhysicalUnaryOperator {
  public:
    PhysicalInferModelOperator(OperatorId id,
                               SchemaPtr inputSchema,
                               SchemaPtr outputSchema,
                               std::string model,
                               std::vector<ExpressionNodePtr> inputFields,
                               std::vector<ExpressionNodePtr> outputFields);

    static PhysicalOperatorPtr create(OperatorId id,
                                      SchemaPtr inputSchema,
                                      SchemaPtr outputSchema,
                                      std::string model,
                                      std::vector<ExpressionNodePtr> inputFields,
                                      std::vector<ExpressionNodePtr> outputFields);

    static PhysicalOperatorPtr create(SchemaPtr inputSchema,
                                      SchemaPtr outputSchema,
                                      std::string model,
                                      std::vector<ExpressionNodePtr> inputFields,
                                      std::vector<ExpressionNodePtr> outputFields);

    std::string toString() const override;
    OperatorNodePtr copy() override;
    const std::string& getModel() const;
    const std::vector<ExpressionNodePtr>& getInputFields() const;
    const std::vector<ExpressionNodePtr>& getOutputFields() const;

  protected:
    const std::string model;
    const std::vector<ExpressionNodePtr> inputFields;
    const std::vector<ExpressionNodePtr> outputFields;
};
}// namespace NES::QueryCompilation::PhysicalOperators

#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALINFERMODELOPERATOR_HPP_
