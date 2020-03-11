#include <Nodes/Operators/LogicalOperators/AggregationLogicalOperatorNode.hpp>

namespace NES {

AggregationLogicalOperatorNode::AggregationLogicalOperatorNode(const AggregationSpec& aggrSpec)
    : aggrSpec_(NES::copy(aggrSpec))
{
}

const std::string AggregationLogicalOperatorNode::toString() const
{
    std::stringstream ss;
    ss << "AGGREGATE(" << NES::toString(aggrSpec_) << ")";
    return ss.str();
}

AggregationLogicalOperatorNode::~AggregationLogicalOperatorNode() {}

/*
bool AggregationLogicalOperatorNode::equal(const Node& rhs) const {
    try {
        auto& rhs_ = dynamic_cast<const AggregationLogicalOperatorNode&>(rhs);
        return true;
    } catch (const std::bad_cast& e) {
        return false;
    }

}
 */

const NodePtr createAggregationLogicalOperatorNode(const AggregationSpec& aggrSpec) {
    return std::make_shared<AggregationLogicalOperatorNode>(aggrSpec);
}

} // namespace NES
