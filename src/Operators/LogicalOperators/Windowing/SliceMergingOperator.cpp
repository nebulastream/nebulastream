#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Windowing/SliceMergingOperator.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
namespace NES {

LogicalOperatorNodePtr createSliceMergingSpecializedOperatorNode(const LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceMergingOperator>(windowDefinition);
}

SliceMergingOperator::SliceMergingOperator(const LogicalWindowDefinitionPtr windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
}

const std::string SliceMergingOperator::toString() const {
    std::stringstream ss;
    ss << "SliceMergingOperator(" << id << ")";
    return ss.str();
}

bool SliceMergingOperator::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<SliceMergingOperator>()->getId() == id;
}

bool SliceMergingOperator::equal(const NodePtr rhs) const {
    return rhs->instanceOf<SliceMergingOperator>();
}

OperatorNodePtr SliceMergingOperator::copy() {
    auto copy = LogicalOperatorFactory::createSliceMergingSpecializedOperator(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
bool SliceMergingOperator::inferSchema() {
    // infer the default input and output schema

    WindowLogicalOperatorNode::inferSchema();

    NES_DEBUG("WindowLogicalOperatorNode: TypeInferencePhase: infer types for window operator with input schema " << inputSchema->toString());
    return true;
}

}// namespace NES
