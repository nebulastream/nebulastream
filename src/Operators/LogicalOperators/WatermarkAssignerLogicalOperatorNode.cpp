#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Optimizer/Utils/OperatorToZ3ExprUtil.hpp>
#include <Windowing/Watermark/WatermarkStrategy.hpp>
#include <z3++.h>

namespace NES {

WatermarkAssignerLogicalOperatorNode::WatermarkAssignerLogicalOperatorNode(const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor, OperatorId id)
    : watermarkStrategyDescriptor(watermarkStrategyDescriptor), LogicalOperatorNode(id) {}


const std::string WatermarkAssignerLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool WatermarkAssignerLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<WatermarkAssignerLogicalOperatorNode>()->getId() == id;

}
bool WatermarkAssignerLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        auto watermarkAssignerOperator = rhs->as<WatermarkAssignerLogicalOperatorNode>();
        return watermarkStrategyDescriptor->equal(watermarkAssignerOperator->getWatermarkStrategyDescriptor());
    }
}
OperatorNodePtr WatermarkAssignerLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

z3::expr WatermarkAssignerLogicalOperatorNode::getZ3Expression(z3::context& context) {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    return OperatorToZ3ExprUtil::createForOperator(operatorNode, context);
}
Windowing::WatermarkStrategyDescriptorPtr WatermarkAssignerLogicalOperatorNode::getWatermarkStrategyDescriptor() const {
    return watermarkStrategyDescriptor;
}

}