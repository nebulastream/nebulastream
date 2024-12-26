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
#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALSenaryOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALSenaryOPERATOR_HPP_

#include <Operators/AbstractOperators/Arity/SenaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <string>

namespace NES {

/**
 * @brief Logical Senary operator, defines three output schemas
 */
class LogicalSenaryOperator : public SenaryOperator {
  public:
    /**
     * @brief Constructs a LogicalSenaryOperator with the given parameters.
     * @param one The first expression node.
      * @param two The second expression node.
      * @param three The third expression node.
      * @param four The fourth expression node.
      * @param five The fifth expression node.
      * @param six The sixth expression node.
     * @param function The function name or identifier.
     * @param id The unique operator identifier.
     */
    LogicalSenaryOperator(const ExpressionNodePtr one,
                            const ExpressionNodePtr two,
                            const ExpressionNodePtr three,
                            const ExpressionNodePtr four,
                            const ExpressionNodePtr five,
                            const ExpressionNodePtr six,
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
    virtual std::vector<OperatorPtr> getoneOperators() const;

    /**
     * @brief Retrieves all middle input operators based on the middle input schema.
     * @return A vector of OperatorPtr representing middle input operators.
     */
    virtual std::vector<OperatorPtr> gettwoOperators() const;

    /**
     * @brief Retrieves all right input operators based on the right input schema.
     * @return A vector of OperatorPtr representing right input operators.
     */
    virtual std::vector<OperatorPtr> getthreeOperators() const;

    /**
     * @brief Retrieves all right input operators based on the right input schema.
     * @return A vector of OperatorPtr representing right input operators.
     */
    virtual std::vector<OperatorPtr> getfourOperators() const;

    /**
     * @brief Retrieves all right input operators based on the right input schema.
     * @return A vector of OperatorPtr representing right input operators.
     */
    virtual std::vector<OperatorPtr> getfiveOperators() const;

    /**
     * @brief Retrieves all right input operators based on the right input schema.
     * @return A vector of OperatorPtr representing right input operators.
     */
    virtual std::vector<OperatorPtr> getsixOperators() const;

    

  private:
    /**
     * @brief Retrieves operators matching the specified schema.
     * @param schema The schema to match against child operators.
     * @return A vector of OperatorPtr that match the given schema.
     */
    std::vector<OperatorPtr> getOperatorsBySchema(const SchemaPtr& schema) const;

  protected:
    const ExpressionNodePtr one;
    const ExpressionNodePtr two;
    const ExpressionNodePtr three;
    const ExpressionNodePtr four;
    const ExpressionNodePtr five;
    const ExpressionNodePtr six;
    std::string function;

    SchemaPtr oneInputSchema = Schema::create();
    SchemaPtr twoInputSchema = Schema::create();
    SchemaPtr threeInputSchema = Schema::create();
    SchemaPtr fourInputSchema = Schema::create();
    SchemaPtr fiveInputSchema = Schema::create();
    SchemaPtr sixInputSchema = Schema::create();
    SchemaPtr outputSchema = Schema::create();

    std::vector<SchemaPtr> distinctSchemas;
    std::vector<OriginId> oneInputOriginIds;
    std::vector<OriginId> twoInputOriginIds;
    std::vector<OriginId> threeInputOriginIds;
    std::vector<OriginId> fourInputOriginIds;
    std::vector<OriginId> fiveInputOriginIds;
    std::vector<OriginId> sixInputOriginIds;
};

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALSenaryOPERATOR_HPP_
