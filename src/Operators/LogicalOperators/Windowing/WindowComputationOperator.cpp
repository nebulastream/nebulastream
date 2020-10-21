#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
namespace NES {

LogicalOperatorNodePtr createWindowComputationSpecializedOperatorNode(const Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<WindowComputationOperator>(windowDefinition);
}

WindowComputationOperator::WindowComputationOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : WindowOperatorNode(windowDefinition) {
    this->windowDefinition->setDistributionCharacteristic(Windowing::DistributionCharacteristic::createCombiningWindowType());
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
bool WindowComputationOperator::inferSchema() {
    WindowOperatorNode::inferSchema();
    // infer the default input and output schema
    NES_DEBUG("WindowComputationOperator: TypeInferencePhase: infer types for window operator with input schema " << inputSchema->toString());

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
        NES_THROW_RUNTIME_ERROR("WindowComputationOperator: type inference for non keyed streams is not supported");
        return false;
    }
}
}// namespace NES