#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalSliceSinkOperator::create(Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return create(UtilityFunctions::getNextOperatorId(), windowDefinition);
}

PhysicalOperatorPtr PhysicalSliceSinkOperator::create(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalSliceSinkOperator>(id, windowDefinition);
}

PhysicalSliceSinkOperator::PhysicalSliceSinkOperator(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), AbstractWindowOperator(windowDefinition), PhysicalUnaryOperator(id){};

const std::string PhysicalSliceSinkOperator::toString() const {
    return "PhysicalSliceSinkOperator";
}

OperatorNodePtr PhysicalSliceSinkOperator::copy() {
    return create(id, windowDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES