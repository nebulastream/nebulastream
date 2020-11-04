#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>

namespace NES {

WatermarkAssignerLogicalOperatorNode::WatermarkAssignerLogicalOperatorNode(const Windowing::WatermarkStrategyPtr watermarkStrategy, OperatorId id)
    : watermarkStrategy(watermarkStrategy), LogicalOperatorNode(id) {}


const std::string WatermarkAssignerLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WATERMARKASSIGNER(" << id << ")";
    return ss.str();
}
bool WatermarkAssignerLogicalOperatorNode::inferSchema() {
    return OperatorNode::inferSchema();
}
bool WatermarkAssignerLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<WatermarkAssignerLogicalOperatorNode>()->getId() == id;

}
bool WatermarkAssignerLogicalOperatorNode::equal(const NodePtr rhs) const {
    return Node::equal(rhs);
}
OperatorNodePtr WatermarkAssignerLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategy, id);
    copy->setId(id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

Windowing::WatermarkStrategyPtr WatermarkAssignerLogicalOperatorNode::getWatermarkStrategy() {
    return watermarkStrategy;
}

}