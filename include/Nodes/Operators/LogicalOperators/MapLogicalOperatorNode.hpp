#ifndef MAP_LOGICAL_OPERATOR_NODE_HPP
#define MAP_LOGICAL_OPERATOR_NODE_HPP

#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>

namespace NES {

/**
 * @brief Map operator, which contains an field assignment expression that manipulates a field of the record.
 */
class MapLogicalOperatorNode : public LogicalOperatorNode {
  public:
    MapLogicalOperatorNode(const FieldAssignmentExpressionNodePtr mapExpression);

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

    const std::string toString() const override;

  private:
    FieldAssignmentExpressionNodePtr mapExpression;
};

typedef std::shared_ptr<MapLogicalOperatorNode> MapLogicalOperatorNodePtr;

}// namespace NES

#endif// MAP_LOGICAL_OPERATOR_NODE_HPP
