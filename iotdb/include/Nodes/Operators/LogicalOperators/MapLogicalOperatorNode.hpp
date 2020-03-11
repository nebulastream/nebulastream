#ifndef MAP_LOGICAL_OPERATOR_NODE_HPP
#define MAP_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>


namespace NES {
class MapLogicalOperatorNode : public LogicalOperatorNode{
public:
    MapLogicalOperatorNode(const AttributeFieldPtr, const PredicatePtr);
   const std::string toString() const override;

private:
    PredicatePtr predicate_;
    AttributeFieldPtr field_;
};
}

#endif  // MAP_LOGICAL_OPERATOR_NODE_HPP
