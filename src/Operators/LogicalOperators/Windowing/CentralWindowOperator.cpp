#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <z3++.h>

namespace NES {

CentralWindowOperator::CentralWindowOperator(const Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id)
    : WindowOperatorNode(windowDefinition, id) {
    windowDefinition->setDistributionCharacteristic(Windowing::DistributionCharacteristic::createCompleteWindowType());
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
    auto copy = LogicalOperatorFactory::createCentralWindowSpecializedOperator(windowDefinition, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}
bool CentralWindowOperator::inferSchema() {

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
    } else {
        NES_THROW_RUNTIME_ERROR("SliceCreationOperator: type inference for non keyed streams is not supported");
    }
}

z3::expr CentralWindowOperator::getFOL() {
    // create a context
    z3::context c;
    z3::expr x = c.int_const("x");
    return x;
}

}// namespace NES
