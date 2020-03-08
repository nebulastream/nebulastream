#include <OperatorNodes/Impl/SourceLogicalOperatorNode.hpp>

namespace NES {

// SourceLogicalOperatorNode::SourceLogicalOperatorNode() {
//     std::cout << "Call default SourceLogicalOperatorNode constructor" << std::endl;
// }

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const DataSourcePtr source) : source_(source) {

}

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const OperatorPtr op) {
    std::cout << "op: " << op->toString() << std::endl;
}

bool SourceLogicalOperatorNode::equals(const Node& rhs) const {
    // try {
    //     auto& rhs_ = dynamic_cast<const SourceLogicalOperatorNode&>(rhs);
    //     return true;
    // } catch (const std::bad_cast& e) {
    //     return false;
    // }
    if (&rhs == this)
        return true;
    return false;
}

OperatorType SourceLogicalOperatorNode::getOperatorType() const {
    return OperatorType::SOURCE_OP;
}

const std::string SourceLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << NES::toString(source_) << ")";
    return ss.str();
}

const NodePtr createSourceLogicalOperatorNode(const DataSourcePtr& source) {
    return std::make_shared<SourceLogicalOperatorNode>(source);
}

}
