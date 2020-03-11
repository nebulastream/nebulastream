#include <Nodes/Operators/LogicalOperators/KeyByLogicalOperatorNode.hpp>
#include <sstream>

namespace NES {

KeyByLogicalOperatorNode::KeyByLogicalOperatorNode(const Attributes& keybySpec) : keyby_spec_(NES::copy(keybySpec)) {}

const std::string KeyByLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "KEYBY(" << NES::toString(keyby_spec_) << ")";
    return ss.str();
}

/*bool KeyByLogicalOperatorNode::equal(const Node& rhs) const {
    try {
        auto& rhs_ = dynamic_cast<const KeyByLogicalOperatorNode&>(rhs);
        return true;
    } catch (const std::bad_cast& e) {
        return false;
    }
}
*/


} // namespace NES
