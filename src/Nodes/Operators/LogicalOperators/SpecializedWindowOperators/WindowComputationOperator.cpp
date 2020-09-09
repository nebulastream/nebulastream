#include <Nodes/Operators/SpecializedWindowOperators/WindowComputationOperator.hpp>
#include <API/Schema.hpp>
#include <API/Window/WindowDefinition.hpp>
namespace NES {

LogicalOperatorNodePtr createWindowComputationSpecializedOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<WindowComputationOperator>(windowDefinition);
}

WindowComputationOperator::WindowComputationOperator(const WindowDefinitionPtr windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
    windowDefinition->setDistributionCharacteristic(DistributionCharacteristic::createCombiningWindowType());
}


const std::string WindowComputationOperator::toString() const {
    std::stringstream ss;
    ss << "WindowComputationOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

bool WindowComputationOperator::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<WindowComputationOperator>()->getId() == id;
}

bool WindowComputationOperator::equal(const NodePtr rhs) const {
    return rhs->instanceOf<WindowComputationOperator>();
}

OperatorNodePtr WindowComputationOperator::copy() {
    auto copy = createWindowComputationSpecializedOperatorNode(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
}// namespace NES