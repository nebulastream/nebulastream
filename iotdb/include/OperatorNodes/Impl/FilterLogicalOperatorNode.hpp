#ifndef FILTER_LOGICAL_OPERATOR_NODE_HPP
#define FILTER_LOGICAL_OPERATOR_NODE_HPP


#include <OperatorNodes/LogicalOperatorNode.hpp>


namespace NES {

class FilterLogicalOperatorNode : public LogicalOperatorNode {
public:
    FilterLogicalOperatorNode();
    FilterLogicalOperatorNode(const OperatorPtr);
    FilterLogicalOperatorNode(const PredicatePtr);

    FilterLogicalOperatorNode(FilterLogicalOperatorNode& other);
    FilterLogicalOperatorNode& operator=(FilterLogicalOperatorNode& other);
    const BaseOperatorNodePtr copy();
    OperatorType getOperatorType() const override;
    const std::string toString() const override;
private:
    PredicatePtr predicate_;

};

}      // namespace NES
#endif // FILTER_LOGICAL_OPERATOR_NODE_HPP
