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

#ifndef NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
#define NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
namespace NES {

class FieldAssignmentExpressionNode;
typedef std::shared_ptr<FieldAssignmentExpressionNode> FieldAssignmentExpressionNodePtr;
/**
 * @brief A FieldAssignmentExpression represents the assignment of an expression result to a specific field.
 */
class FieldAssignmentExpressionNode : public BinaryExpressionNode {
  public:
    FieldAssignmentExpressionNode(DataTypePtr stamp);

    /**
     * @brief Create untyped field read.
     */
    static FieldAssignmentExpressionNodePtr create(FieldAccessExpressionNodePtr fieldAccess, ExpressionNodePtr expressionNodePtr);

    const std::string toString() const override;
    bool equal(const NodePtr rhs) const override;

    /**
     * @brief return the field to which a new value is assigned.
     * @return FieldAccessExpressionNodePtr
     */
    FieldAccessExpressionNodePtr getField() const;
    /**
     * @brief returns the expressions, which calculates the new value.
     * @return ExpressionNodePtr
     */
    ExpressionNodePtr getAssignment() const;

    void inferStamp(SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

  protected:
    explicit FieldAssignmentExpressionNode(FieldAssignmentExpressionNode* other);
};
}// namespace NES

#endif// NES_INCLUDE_NODES_EXPRESSIONS_FIELDASSIGNMENTEXPRESSION_HPP_
