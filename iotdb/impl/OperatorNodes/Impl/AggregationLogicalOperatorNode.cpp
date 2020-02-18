#include <OperatorNodes/Impl/AggregationLogicalOperatorNode.hpp>

namespace NES {

AggregationLogicalOperatorNode::AggregationLogicalOperatorNode(const AggregationSpec& aggrSpec)
    : aggrSpec_(NES::copy(aggrSpec))
{
}

// AggregationLogicalOperatorNode::AggregationLogicalOperatorNode(const AggregationLogicalOperatorNode& other) : aggr_spec_(NES::copy(other.aggr_spec_))
// {
// }

// AggregationLogicalOperatorNode& AggregationLogicalOperatorNode::operator=(const AggregationLogicalOperatorNode& other)
// {
//     if (this != &other) {
//         aggr_spec_ = NES::copy(other.aggr_spec_);
//     }
//     return *this;
// }

// const BaseOperatorNodePtr AggregationLogicalOperatorNode::copy() const { return std::make_shared<AggregationLogicalOperatorNode>(*this); }

const std::string AggregationLogicalOperatorNode::toString() const
{
    std::stringstream ss;
    ss << "AGGREGATE(" << NES::toString(aggrSpec_) << ")";
    return ss.str();
}

OperatorType AggregationLogicalOperatorNode::getOperatorType() const {
    return OperatorType::AGGREGATION_OP;
}

AggregationLogicalOperatorNode::~AggregationLogicalOperatorNode() {}

const BaseOperatorNodePtr createAggregationLogicalOperatorNode(const AggregationSpec& aggrSpec) {
    return std::make_shared<AggregationLogicalOperatorNode>(aggrSpec);
}

} // namespace NES
