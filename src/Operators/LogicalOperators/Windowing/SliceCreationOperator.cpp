#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
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

LogicalOperatorNodePtr createSliceCreationSpecializedOperatorNode(const Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<SliceCreationOperator>(windowDefinition);
}

SliceCreationOperator::SliceCreationOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : WindowOperatorNode(windowDefinition) {
    windowDefinition->setDistributionCharacteristic(Windowing::DistributionCharacteristic::createSlicingWindowType());
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
bool SliceCreationOperator::inferSchema() {
    WindowOperatorNode::inferSchema();
    // infer the default input and output schema
    NES_DEBUG("SliceCreationOperator: TypeInferencePhase: infer types for window operator with input schema " << inputSchema->toString());

    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    windowAggregation->inferStamp(inputSchema);

    auto windowType = windowDefinition->getWindowType();
    if (windowDefinition->isKeyed()) {
        // infer the data type of the key field.
        windowDefinition->getOnKey()->inferStamp(inputSchema);
        outputSchema = Schema::create()
            ->addField(createField("start", UINT64))
            ->addField(createField("end", UINT64))
            ->addField(AttributeField::create(windowDefinition->getOnKey()->getFieldName(), windowDefinition->getOnKey()->getStamp()))
            ->addField(AttributeField::create(windowAggregation->as()->as<FieldAccessExpressionNode>()->getFieldName(), windowAggregation->on()->getStamp()));
        return true;
    }else{
        NES_THROW_RUNTIME_ERROR("SliceCreationOperator: type inference for non keyed streams is not supported");
        return false;
    }


}

}// namespace NES
