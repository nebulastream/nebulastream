
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>

namespace NES {

SourceLogicalOperatorNode::SourceLogicalOperatorNode(const SourceDescriptorPtr sourceDescriptor, OperatorId id)
    : sourceDescriptor(sourceDescriptor), LogicalOperatorNode(id) {}

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
    ss << "SOURCE(" << id << ")";
    return ss.str();
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
    auto copy = LogicalOperatorFactory::createSourceOperator(sourceDescriptor, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

}// namespace NES
