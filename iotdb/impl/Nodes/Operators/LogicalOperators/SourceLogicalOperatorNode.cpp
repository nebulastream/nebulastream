#include <Nodes/Operators/LogicalOperators/SourceLogicalOperatorNode.hpp>
#include <API/InputQuery.hpp>

namespace NES {

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const DataSourcePtr source)
    : LogicalOperatorNode(), source_(source) {
}

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const SourceLogicalOperatorNode* source)
    : LogicalOperatorNode(), source_(source->source_) {
}

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const NodePtr op) {
    std::cout << "op: " << op->toString() << std::endl;
}
bool SourceLogicalOperatorNode::equal(const NodePtr& rhs) const {
    // try {
    //     auto& rhs_ = dynamic_cast<const SourceLogicalOperatorNode&>(rhs);
    //     return true;
    // } catch (const std::bad_cast& e) {
    //     return false;
    // }
    if (isIdentical(rhs))
        return true;
    return false;
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
