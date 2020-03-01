#ifndef MAP_LOGICAL_OPERATOR_NODE_HPP
#define MAP_LOGICAL_OPERATOR_NODE_HPP

#include <OperatorNodes/LogicalOperatorNode.hpp>


namespace NES {
class MapLogicalOperatorNode : public LogicalOperatorNode {
public:
    MapLogicalOperatorNode(const AttributeFieldPtr, const PredicatePtr);
    OperatorType getOperatorType() const override;
    const std::string toString() const override;

    virtual bool equals(const BaseOperatorNode& rhs) const override;
private:
    PredicatePtr predicate_;
    AttributeFieldPtr field_;
};
}

#endif  // MAP_LOGICAL_OPERATOR_NODE_HPP
