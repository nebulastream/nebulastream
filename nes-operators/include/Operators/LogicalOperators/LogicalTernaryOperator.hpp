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
#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALTERNARYOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALTERNARYOPERATOR_HPP_

#include <Operators/AbstractOperators/Arity/TernaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <string>

namespace NES {

/**
 * @brief Logical Ternary operator, defines three output schemas
 */
class LogicalTernaryOperator : public TernaryOperator {
  public:
    /**
     * @brief Constructs a LogicalTernaryOperator with the given parameters.
     * @param left The left expression node.
     * @param middle The middle expression node.
     * @param right The right expression node.
     * @param function The function name or identifier.
     * @param id The unique operator identifier.
     */
    LogicalTernaryOperator(const ExpressionNodePtr left,
                           const ExpressionNodePtr middle,
                           const ExpressionNodePtr right,
                           const std::string function,
                           OperatorId id);

    /**
     * @brief Creates a copy of this operator.
     * @return A shared pointer to the copied operator.
     */
    virtual OperatorPtr copy() override;

    /**
     * @brief Infers the input and output schema of this operator depending on its children.
     * @throws TypeInferenceException if the schema could not be inferred correctly or if the inferred types are not valid.
     * @return true if the schema was correctly inferred.
     */
    virtual bool inferSchema();

    /**
     * @brief Infers the origins of the inputs based on child operators.
     */
    virtual void inferInputOrigins();

    /**
     * @brief Retrieves all left input operators based on the left input schema.
     * @return A vector of OperatorPtr representing left input operators.
     */
    virtual std::vector<OperatorPtr> getLeftOperators();

    /**
     * @brief Retrieves all right input operators based on the right input schema.
     * @return A vector of OperatorPtr representing right input operators.
     */
    virtual std::vector<OperatorPtr> getRightOperators();

  private:
    /**
     * @brief Retrieves operators matching the specified schema.
     * @param schema The schema to match against child operators.
     * @return A vector of OperatorPtr that match the given schema.
     */
    std::vector<OperatorPtr> getOperatorsBySchema(const SchemaPtr& schema) const;

  protected:
    const ExpressionNodePtr left;
    const ExpressionNodePtr middle;
    const ExpressionNodePtr right;
    std::string function;

    SchemaPtr leftInputSchema = Schema::create();
    SchemaPtr middleInputSchema = Schema::create();
    SchemaPtr rightInputSchema = Schema::create();
    SchemaPtr outputSchema = Schema::create();

    std::vector<SchemaPtr> distinctSchemas;
    std::vector<OriginId> leftInputOriginIds;
    std::vector<OriginId> middleInputOriginIds;
    std::vector<OriginId> rightInputOriginIds;
};

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALTERNARYOPERATOR_HPP_
