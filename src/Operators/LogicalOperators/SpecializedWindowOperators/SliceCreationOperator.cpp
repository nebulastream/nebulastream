#include <API/Schema.hpp>
#include <Operators/LogicalOperators/SpecializedWindowOperators/SliceCreationOperator.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/WindowDefinition.hpp>
namespace NES {

LogicalOperatorNodePtr createSliceCreationSpecializedOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceCreationOperator>(windowDefinition);
}

SliceCreationOperator::SliceCreationOperator(const WindowDefinitionPtr windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
    windowDefinition->setDistributionCharacteristic(DistributionCharacteristic::createSlicingWindowType());
}

const std::string SliceCreationOperator::toString() const {
    std::stringstream ss;
    ss << "SliceCreationOperator(" << id << ")";
    return ss.str();
}

bool SliceCreationOperator::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<SliceCreationOperator>()->getId() == id;
}

bool SliceCreationOperator::equal(const NodePtr rhs) const {
    return rhs->instanceOf<SliceCreationOperator>();
}

OperatorNodePtr SliceCreationOperator::copy() {
    auto copy = LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

}// namespace NES
