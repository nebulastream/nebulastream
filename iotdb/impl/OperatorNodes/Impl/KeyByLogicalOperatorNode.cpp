#include <OperatorNodes/Impl/KeyByLogicalOperatorNode.hpp>

namespace NES {

KeyByLogicalOperatorNode::KeyByLogicalOperatorNode(const Attributes& keybySpec) : keyby_spec_(NES::copy(keybySpec)) {}

const std::string KeyByLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "KEYBY(" << NES::toString(keyby_spec_) << ")";
    return ss.str();
}

OperatorType KeyByLogicalOperatorNode::getOperatorType() const {
    return OperatorType::KEYBY_OP;
}

bool KeyByLogicalOperatorNode::equals(const BaseOperatorNode& rhs) const {
    try {
        auto& rhs_ = dynamic_cast<const KeyByLogicalOperatorNode&>(rhs);
        return true;
    } catch (const std::bad_cast& e) {
        return false;
    }
}

const BaseOperatorNodePtr createKeyByLogicalOperatorNode(const Attributes& keybySpec) {
    return std::make_shared<KeyByLogicalOperatorNode>(keybySpec);
}

} // namespace NES
