#include <Nodes/Operators/RefinementOperators/DistributedWindowOperator.hpp>

namespace NES {

LogicalOperatorNodePtr createDistributedWindowLogicalOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<DistributedWindowOperator>(windowDefinition);
}

DistributedWindowOperator::DistributedWindowOperator(const WindowDefinitionPtr& windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition) {
}
}// namespace NES
