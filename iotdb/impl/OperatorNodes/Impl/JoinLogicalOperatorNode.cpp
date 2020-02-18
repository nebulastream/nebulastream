#include <OperatorNodes/Impl/JoinLogicalOperatorNode.hpp>

namespace NES {

JoinLogicalOperatorNode::JoinLogicalOperatorNode(const JoinPredicatePtr joinSpec) : join_spec_(NES::copy(joinSpec)) {}

const std::string JoinLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "JOIN(" << NES::toString(join_spec_) << ")";
    return ss.str();
}

OperatorType JoinLogicalOperatorNode::getOperatorType() const {
    return OperatorType::JOIN_OP;
}

const BaseOperatorNodePtr createJoinLogicalOperatorNode(const JoinPredicatePtr& joinSpec) {
    return std::make_shared<JoinLogicalOperatorNode>(joinSpec);
}

} // namespace NES
