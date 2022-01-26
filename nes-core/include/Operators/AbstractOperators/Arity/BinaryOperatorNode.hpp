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

#ifndef NES_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_BINARYOPERATORNODE_HPP_
#define NES_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_BINARYOPERATORNODE_HPP_

#include <Operators/OperatorForwardDeclaration.hpp>
#include <Operators/OperatorNode.hpp>
namespace NES {

/**
 * @brief A binary operator with more the none input operator, thus it has a left and a right input schema.
 */
class BinaryOperatorNode : public virtual OperatorNode {
  public:
    explicit BinaryOperatorNode(OperatorId id);

    /**
     * @brief detect if this operator is a binary operator, i.e., it has two children
     * @return true if n-ary else false;
     */
    bool isBinaryOperator() const override;

    /**
    * @brief detect if this operator is an binary operator, i.e., it has only one child
    * @return true if n-ary else false;
    */
    bool isUnaryOperator() const override;

    /**
   * @brief detect if this operator is an exchange operator, i.e., it sends it output to multiple parents
   * @return true if n-ary else false;
   */
    bool isExchangeOperator() const override;

    /**
   * @brief get the input schema of this operator from the left side
   * @return SchemaPtr
   */
    SchemaPtr getLeftInputSchema() const;

    /**
    * @brief set the input schema of this operator for the left side
     * @param inputSchema
    */
    void setLeftInputSchema(SchemaPtr inputSchema);

    /**
    * @brief get the input schema of this operator from the left side
    * @return SchemaPtr
    */
    SchemaPtr getRightInputSchema() const;

    /**
     * @brief set the input schema of this operator for the right side
     * @param inputSchema
    */
    void setRightInputSchema(SchemaPtr inputSchema);

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

  protected:
    SchemaPtr leftInputSchema;
    SchemaPtr rightInputSchema;
    SchemaPtr outputSchema;
    std::vector<SchemaPtr> distinctSchemas;
};

}// namespace NES

#endif  // NES_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_BINARYOPERATORNODE_HPP_
