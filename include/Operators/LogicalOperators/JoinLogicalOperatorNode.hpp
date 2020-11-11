#ifndef Join_LOGICAL_OPERATOR_NODE_HPP
#define Join_LOGICAL_OPERATOR_NODE_HPP

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <memory>
#include <z3++.h>

namespace NES {

class JoinLogicalOperatorNode;
typedef std::shared_ptr<JoinLogicalOperatorNode> JoinLogicalOperatorNodePtr;

/**
 * @brief Join operator, which contains an expression as a predicate.
 */
class JoinLogicalOperatorNode : public LogicalOperatorNode {
  public:
    explicit JoinLogicalOperatorNode(Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id);
    ~JoinLogicalOperatorNode() = default;

    bool isIdentical(NodePtr rhs) const override;
    const std::string toString() const override;
    //infer schema of two child operators
    bool inferSchema() override;
    OperatorNodePtr copy() override;
    bool equal(const NodePtr rhs) const override;

    /**
    * @brief Get the First Order Logic formula representation for the logical operator
    * @return and object of type Z3::expr.
    */
    z3::expr getZ3Expression(z3::context& context) override;


    Join::LogicalJoinDefinitionPtr getJoinDefinition();

  private:
    Join::LogicalJoinDefinitionPtr joinDefinition;
};
}// namespace NES
#endif// Join_LOGICAL_OPERATOR_NODE_HPP