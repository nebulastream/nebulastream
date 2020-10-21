#include <API/Schema.hpp>
#include <Operators/SpecializedWindowOperators/WindowComputationOperator.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/WindowDefinition.hpp>
namespace NES {

LogicalOperatorNodePtr createWindowComputationSpecializedOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<WindowComputationOperator>(windowDefinition);
}

WindowComputationOperator::WindowComputationOperator(const WindowDefinitionPtr windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
    this->windowDefinition->setDistributionCharacteristic(DistributionCharacteristic::createCombiningWindowType());
    this->windowDefinition->setNumberOfInputEdges(windowDefinition->getNumberOfInputEdges());
}

const std::string WindowComputationOperator::toString() const {
    std::stringstream ss;
    ss << "WindowComputationOperator(" << id << ")";
    return ss.str();
}

bool WindowComputationOperator::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<WindowComputationOperator>()->getId() == id;
}

bool WindowComputationOperator::equal(const NodePtr rhs) const {
    return rhs->instanceOf<WindowComputationOperator>();
}

OperatorNodePtr WindowComputationOperator::copy() {
    auto copy = LogicalOperatorFactory::createWindowComputationSpecializedOperator(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
}// namespace NES