#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <API/InputQuery.hpp>

namespace NES {

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor)
    : sourceDescriptor(sourceDescriptor) {}

bool SourceLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<SourceLogicalOperatorNode>()) {
        auto sourceOperator = rhs->as<SourceLogicalOperatorNode>();
        return true;
    }
    return false;
}

const std::string SourceLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << sourceDescriptor->getType() << ")";
    return ss.str();
}

LogicalOperatorNodePtr createSourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor) {
    return std::make_shared<SourceLogicalOperatorNode>(sourceDescriptor);
}

SourceDescriptorPtr SourceLogicalOperatorNode::getSourceDescriptor() {
    return sourceDescriptor;
}
SchemaPtr SourceLogicalOperatorNode::getResultSchema() const {
    return sourceDescriptor->getSchema();
}

}
