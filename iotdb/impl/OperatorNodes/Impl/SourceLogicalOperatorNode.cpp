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

// const BaseOperatorNodePtr SourceLogicalOperatorNode::copy() {
//     return std::make_shared<SourceLogicalOperatorNode>();
// }

OperatorType SourceLogicalOperatorNode::getOperatorType() const {
    return OperatorType::SOURCE_OP;
}
const std::string SourceLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << NES::toString(source_) << ")";
    return ss.str();
}

const BaseOperatorNodePtr createSourceLogicalOperatorNode(const DataSourcePtr& source) {
    return std::make_shared<SourceLogicalOperatorNode>(source);
}

}
