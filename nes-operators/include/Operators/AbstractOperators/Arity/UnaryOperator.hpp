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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_UNARYOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_UNARYOPERATOR_HPP_

#include <Operators/OperatorForwardDeclaration.hpp>
#include <Operators/Operator.hpp>

namespace NES {

/**
 * @brief A unary operator with one input operator, it has exactly one input operator.
 * This virtually inheritances for Operator
 * https://en.wikipedia.org/wiki/Virtual_inheritance
 */
class UnaryOperator : public virtual Operator {
  public:
    explicit UnaryOperator(OperatorId id);
    ~UnaryOperator() noexcept override = default;

    /**
   * @brief get the input schema of this operator
   * @return SchemaPtr
   */
    SchemaPtr getInputSchema() const;

    /**
     * @brief set the input schema of this operator
     * @param inputSchema
    */
    void setInputSchema(SchemaPtr inputSchema);

    /**
    * @brief get the result schema of this operator
    * @return SchemaPtr
    */
    SchemaPtr getOutputSchema() const override;

    /**
     * @brief set the result schema of this operator
     * @param outputSchema
    */
    void setOutputSchema(SchemaPtr outputSchema) override;

    /**
     * @brief Set the input origin ids from the input stream
     * @param originIds
     */
    void setInputOriginIds(std::vector<OriginId> originIds);

    /**
     * @brief Gets the input origin ids  from the input stream
     * @return std::vector<OriginId>
     */
    const std::vector<OriginId> getInputOriginIds() const;

    /**
     * @brief Gets the output origin ids from this operator
     * @return std::vector<OriginId>
     */
    virtual const std::vector<OriginId> getOutputOriginIds() const override;

    /**
     * @brief returns the string representation of the class
     * @return the string representation of the class
     */
    std::string toString() const override;

  protected:
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;
    std::vector<OriginId> inputOriginIds;
};

}// namespace NES

#endif // NES_OPERATORS_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_UNARYOPERATOR_HPP_
