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
    : windowDefinition(windowDefinition) {
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

    // infer the default input and output schema
    OperatorNode::inferSchema();

    auto windowType = windowDefinition->getWindowType();
    if (windowDefinition->isKeyed()) {
        // check if key exist on input schema
        auto key = windowDefinition->getOnKey();
        //if (!inputSchema->get(key->name)->toString().empty()) {
        if (!inputSchema->has(key->name)) {
            NES_ERROR("inferSchema() Window Operator: key  field " << key->name << " does not exist!");
            return false;
        }
    }
    // check if aggregation field exist
    auto windowAggregation = windowDefinition->getWindowAggregation();
    if (!inputSchema->has(windowAggregation->on()->getFieldName())) {
        NES_ERROR("Window Operator: aggregation field dose not exist!");
        return false;
    }

    // create result schema
    windowAggregation->inferStamp(inputSchema);
    //    outputSchema->addField(createField("start", UINT64))->addField(createField("end", UINT64))->addField(createField("key", INT64))->addField(AttributeField::create(windowAggregation->as()->name, aggregationField->dataType));
    outputSchema = Schema::create()
                       ->addField(createField("start", UINT64))
                       ->addField(createField("end", UINT64))
                       ->addField(createField("key", INT64))
                       ->addField(AttributeField::create(windowAggregation->as()->getFieldName(), windowAggregation->on()->getStamp()));
    //    outputSchema = Schema::create()->addField(createField("start", UINT64))->addField(createField("end", UINT64))->addField(createField("key", INT64))->addField("value", INT64);
    //    outputSchema->addField(AttributeField::create(windowAggregation->as()->name, aggregationField->dataType));
    return true;
}

}// namespace NES
