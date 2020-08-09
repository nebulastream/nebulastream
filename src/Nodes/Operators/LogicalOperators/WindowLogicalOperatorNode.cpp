#include <API/Schema.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <sstream>

namespace NES {

WindowLogicalOperatorNode::WindowLogicalOperatorNode(const WindowDefinitionPtr& windowDefinition)
    : windowDefinition(windowDefinition) {
}

const std::string WindowLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WINDOW(" << outputSchema->toString() << ")";
    return ss.str();
}

const WindowDefinitionPtr& WindowLogicalOperatorNode::getWindowDefinition() const {
    return windowDefinition;
}

bool WindowLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<WindowLogicalOperatorNode>()->getId() == id;
}

bool WindowLogicalOperatorNode::equal(const NodePtr rhs) const {
    return rhs->instanceOf<WindowLogicalOperatorNode>();
}

OperatorNodePtr WindowLogicalOperatorNode::copy() {
    auto copy = createWindowLogicalOperatorNode(windowDefinition);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

bool WindowLogicalOperatorNode::inferSchema() {

    // infer the default input and output schema
    OperatorNode::inferSchema();

    auto windowType = windowDefinition->windowType;
    if (windowDefinition->isKeyed()) {
        // check if key exist on input schema
        auto key = windowDefinition->onKey;
        if (!inputSchema->has(key->name)) {
            NES_ERROR("Window Operator: key field dose not exist!");
            return false;
        }
    }
    // check if aggregation field exist
    auto windowAggregation = windowDefinition->windowAggregation;
    if (!inputSchema->has(windowAggregation->on()->name)) {
        NES_ERROR("Window Operator: aggregation field dose not exist!");
        return false;
    }
    
    // create result schema
    auto aggregationField = inputSchema->get(windowAggregation->on()->name);
    outputSchema = Schema::create();
    outputSchema->addField(AttributeField::create(windowAggregation->as()->name, aggregationField->dataType));
    return true;
}

LogicalOperatorNodePtr createWindowLogicalOperatorNode(const WindowDefinitionPtr windowDefinitionPtr) {
    return std::make_shared<WindowLogicalOperatorNode>(windowDefinitionPtr);
}

}// namespace NES
