#ifndef MAP_LOGICAL_OPERATOR_NODE_HPP
#define MAP_LOGICAL_OPERATOR_NODE_HPP

#include <memory>
#include <OperatorNodes/LogicalOperatorNode.hpp>


namespace NES {
class MapLogicalOperatorNode : public LogicalOperatorNode,
                               public std::enable_shared_from_this<MapLogicalOperatorNode> {
public:
    MapLogicalOperatorNode(const AttributeFieldPtr, const PredicatePtr);
    OperatorType getOperatorType() const override;
    const std::string toString() const override;

    virtual NodePtr makeShared() override { return shared_from_this(); };
    virtual bool equals(const Node& rhs) const override;
private:
    PredicatePtr predicate_;
    AttributeFieldPtr field_;
};
}

#endif  // MAP_LOGICAL_OPERATOR_NODE_HPP
