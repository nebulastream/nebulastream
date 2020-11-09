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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FIELDACCESSEXPRESSIONNODE_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FIELDACCESSEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
/**
 * @brief A FieldAccessExpression reads a specific field of the current record.
 * It can be created typed or untyped.
 */
class FieldAccessExpressionNode : public ExpressionNode {
  public:
    /**
    * @brief Create typed field read.
    */
    static ExpressionNodePtr create(DataTypePtr stamp, std::string fieldName);

    /**
     * @brief Create untyped field read.
     */
    static ExpressionNodePtr create(std::string fieldName);

    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;

    const std::string getFieldName();
    void setFieldName(std::string name);

    /**
    * @brief Infers the stamp of the expression given the current schema.
    * @param SchemaPtr
    */
    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit FieldAccessExpressionNode(FieldAccessExpressionNode* other);

  private:
    FieldAccessExpressionNode(DataTypePtr stamp, std::string fieldName);
    /**
     * @brief Name of the field want to access.
     */
    std::string fieldName;
};

typedef std::shared_ptr<FieldAccessExpressionNode> FieldAccessExpressionNodePtr;

}// namespace NES

#endif// NES_INCLUDE_NODES_EXPRESSIONS_FIELDACCESSEXPRESSIONNODE_HPP_
