#include <Nodes/Operators/SpecializedWindowOperators/CentralWindowOperator.hpp>
#include <API/Schema.hpp>
#include <API/Window/WindowDefinition.hpp>

namespace NES {

LogicalOperatorNodePtr createCentralWindowSpecializedOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<CentralWindowOperator>(windowDefinition);
}

CentralWindowOperator::CentralWindowOperator(const WindowDefinitionPtr windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
}


const std::string CentralWindowOperator::toString() const {
    std::stringstream ss;
    ss << "CENTRALWINDOW(" << outputSchema->toString() << ")";
    return ss.str();
}

bool CentralWindowOperator::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<CentralWindowOperator>()->getId() == id;
}

bool CentralWindowOperator::equal(const NodePtr rhs) const {
    return rhs->instanceOf<CentralWindowOperator>();
}

OperatorNodePtr CentralWindowOperator::copy() {
    auto copy = createCentralWindowSpecializedOperatorNode(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}


}// namespace NES
