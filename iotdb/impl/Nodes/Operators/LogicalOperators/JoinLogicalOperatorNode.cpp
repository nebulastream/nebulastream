#include <Nodes/Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>

namespace NES {

JoinLogicalOperatorNode::JoinLogicalOperatorNode(const JoinPredicatePtr joinSpec) : join_spec_(NES::copy(joinSpec)) {}

const std::string JoinLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "JOIN(" << NES::toString(join_spec_) << ")";
    return ss.str();
}

/*
bool JoinLogicalOperatorNode::equal(const Node& rhs) const {
    try {
        auto& rhs_ = dynamic_cast<const JoinLogicalOperatorNode&>(rhs);
        return true;
    } catch (const std::bad_cast& e) {
        return false;
    }
}
*/
const NodePtr createJoinLogicalOperatorNode(const JoinPredicatePtr& joinSpec) {
    return std::make_shared<JoinLogicalOperatorNode>(joinSpec);
}

} // namespace NES
