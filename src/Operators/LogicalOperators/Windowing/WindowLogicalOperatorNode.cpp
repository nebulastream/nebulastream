#include <API/Schema.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <sstream>

namespace NES {

WindowLogicalOperatorNode::WindowLogicalOperatorNode(const LogicalWindowDefinitionPtr windowDefinition)
    :  WindowOperatorNode(windowDefinition) {
}

const std::string WindowLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WINDOW(" << id << ")";
    return ss.str();
}

const LogicalWindowDefinitionPtr& WindowLogicalOperatorNode::getWindowDefinition() const {
    return windowDefinition;
}

bool WindowLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    bool eq = equal(rhs);
    bool idCmp = rhs->as<WindowLogicalOperatorNode>()->getId() == id;
    bool typeInfer = rhs->instanceOf<CentralWindowOperator>();
    return eq && idCmp && !typeInfer;
}

bool WindowLogicalOperatorNode::equal(const NodePtr rhs) const {
    return rhs->instanceOf<WindowLogicalOperatorNode>();
}

OperatorNodePtr WindowLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createWindowOperator(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

bool WindowLogicalOperatorNode::inferSchema() {

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
