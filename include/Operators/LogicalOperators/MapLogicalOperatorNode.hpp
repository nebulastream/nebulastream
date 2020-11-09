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

#ifndef MAP_LOGICAL_OPERATOR_NODE_HPP
#define MAP_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>
#include <z3++.h>

namespace NES {

/**
 * @brief Map operator, which contains an field assignment expression that manipulates a field of the record.
 */
class MapLogicalOperatorNode : public LogicalOperatorNode {
  public:
    MapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression, OperatorId id);

    /**
     * @brief Returns the expression of this map operator
     * @return FieldAssignmentExpressionNodePtr
     */
    FieldAssignmentExpressionNodePtr getMapExpression();

    /**
     * @brief Infers the schema of the map operator. We support two cases:
     * 1. the assignment statement manipulates a already existing field. In this case the data type of the field can change.
     * 2. the assignment statement creates a new field with an inferred data type.
     * @throws throws exception if inference was not possible.
     * @return true if inference was possible
     */
    bool inferSchema() override;
    bool equal(const NodePtr rhs) const override;
    bool isIdentical(NodePtr rhs) const override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
    void inferZ3Expression(z3::ContextPtr context) override;

  private:
    FieldAssignmentExpressionNodePtr mapExpression;
};

typedef std::shared_ptr<MapLogicalOperatorNode> MapLogicalOperatorNodePtr;

}// namespace NES

#endif// MAP_LOGICAL_OPERATOR_NODE_HPP
