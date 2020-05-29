#include <API/InputQuery.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>

namespace NES {

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor)
    : sourceDescriptor(sourceDescriptor) {}

bool SourceLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (this->isIdentical(rhs)) {
        return true;
    }
    if (rhs->instanceOf<SourceLogicalOperatorNode>()) {
        auto sourceOperator = rhs->as<SourceLogicalOperatorNode>();
        return sourceOperator->getSourceDescriptor()->equal(sourceDescriptor);
    }
    return false;
}

const std::string SourceLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SOURCE(" << outputSchema->toString() << ")";
    return ss.str();
}

LogicalOperatorNodePtr createSourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor) {
    return std::make_shared<SourceLogicalOperatorNode>(sourceDescriptor);
}

SourceDescriptorPtr SourceLogicalOperatorNode::getSourceDescriptor() {
    return sourceDescriptor;
}
bool SourceLogicalOperatorNode::inferSchema() {
    inputSchema = sourceDescriptor->getSchema();
    outputSchema = sourceDescriptor->getSchema();
}
void SourceLogicalOperatorNode::setSourceDescriptor(SourceDescriptorPtr sourceDescriptor) {
    this->sourceDescriptor = sourceDescriptor;
}

}// namespace NES
