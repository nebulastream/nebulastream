#include <Nodes/Operators/SpecializedWindowOperators/SliceCreationOperator.hpp>
#include <API/Schema.hpp>
#include <API/Window/WindowDefinition.hpp>
namespace NES {

LogicalOperatorNodePtr createSliceCreationSpecializedOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceCreationOperator>(windowDefinition);
}

SliceCreationOperator::SliceCreationOperator(const WindowDefinitionPtr windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
}

const std::string SliceCreationOperator::toString() const {
    std::stringstream ss;
    ss << "SliceCreationOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

bool SliceCreationOperator::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<SliceCreationOperator>()->getId() == id;
}

bool SliceCreationOperator::equal(const NodePtr rhs) const {
    return rhs->instanceOf<SliceCreationOperator>();
}

OperatorNodePtr SliceCreationOperator::copy() {
    auto copy = createSliceCreationSpecializedOperatorNode(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

}// namespace NES
