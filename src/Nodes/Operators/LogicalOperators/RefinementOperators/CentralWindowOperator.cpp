#include <Nodes/Operators/RefinementOperators/CentralWindowOperator.hpp>

namespace NES {

LogicalOperatorNodePtr createCentralWindowLogicalOperatorNode(const WindowDefinitionPtr windowDefinition) {
    return std::make_shared<CentralWindowOperator>(windowDefinition);
}

CentralWindowOperator::CentralWindowOperator(const WindowDefinitionPtr& windowDefinition)
    : WindowLogicalOperatorNode(windowDefinition){
}
}// namespace NES
