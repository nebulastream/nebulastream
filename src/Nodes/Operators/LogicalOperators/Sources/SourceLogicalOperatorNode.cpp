
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <API/Schema.hpp>

namespace NES {

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor)
    : sourceDescriptor(sourceDescriptor) {}

bool SourceLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<SourceLogicalOperatorNode>()->getId() == id;
}

bool SourceLogicalOperatorNode::equal(const NodePtr rhs) const {
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
    return true;
}
void SourceLogicalOperatorNode::setSourceDescriptor(SourceDescriptorPtr sourceDescriptor) {
    this->sourceDescriptor = sourceDescriptor;
}

OperatorNodePtr SourceLogicalOperatorNode::copy() {
    auto copy = createSourceLogicalOperatorNode(sourceDescriptor);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

}// namespace NES
