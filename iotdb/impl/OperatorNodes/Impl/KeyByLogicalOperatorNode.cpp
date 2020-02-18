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

const BaseOperatorNodePtr createKeyByLogicalOperatorNode(const Attributes& keybySpec) {
    return std::make_shared<KeyByLogicalOperatorNode>(keybySpec);
}

} // namespace NES
