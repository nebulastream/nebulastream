#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
namespace NES {

LogicalOperatorNodePtr createSliceCreationSpecializedOperatorNode(const LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceCreationOperator>(windowDefinition);
}

SliceCreationOperator::SliceCreationOperator(const LogicalWindowDefinitionPtr windowDefinition)
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
