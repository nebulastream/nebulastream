#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Nodes/Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

namespace NES {

LogicalOperatorNodePtr createCentralWindowSpecializedOperatorNode(const LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<CentralWindowOperator>(windowDefinition);
}

CentralWindowOperator::CentralWindowOperator(const LogicalWindowDefinitionPtr windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
    windowDefinition->setDistributionCharacteristic(DistributionCharacteristic::createCompleteWindowType());
}

const std::string CentralWindowOperator::toString() const {
    std::stringstream ss;
    ss << "CENTRALWINDOW(" << id << ")";
    return ss.str();
}

bool CentralWindowOperator::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<CentralWindowOperator>()->getId() == id;
}

bool CentralWindowOperator::equal(const NodePtr rhs) const {
    return rhs->instanceOf<CentralWindowOperator>();
}

OperatorNodePtr CentralWindowOperator::copy() {
    auto copy = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
bool CentralWindowOperator::inferSchema() {

    WindowLogicalOperatorNode::inferSchema();

    NES_DEBUG("CentralWindowOperator: TypeInferencePhase: infer types for window operator with input schema " << inputSchema->toString());

    auto windowType = windowDefinition->getWindowType();
    if (windowDefinition->isKeyed()) {
        // infer the data type of the key field.
        windowDefinition->getOnKey()->inferStamp(inputSchema);
    }
    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    windowAggregation->inferStamp(inputSchema);

    outputSchema = Schema::create()
        ->addField(createField("start", UINT64))
        ->addField(createField("end", UINT64))
        ->addField(AttributeField::create("key", windowAggregation->on()->getStamp()))
        ->addField(AttributeField::create(windowAggregation->as()->getFieldName(), windowAggregation->on()->getStamp()));

    return true;
}

}// namespace NES
