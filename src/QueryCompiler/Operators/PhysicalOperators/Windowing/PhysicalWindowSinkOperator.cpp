#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalWindowSinkOperator::create(Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return create(UtilityFunctions::getNextOperatorId(), windowDefinition);
}

PhysicalOperatorPtr PhysicalWindowSinkOperator::create(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalWindowSinkOperator>(id, windowDefinition);
}

PhysicalWindowSinkOperator::PhysicalWindowSinkOperator(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), AbstractWindowOperator(windowDefinition), PhysicalUnaryOperator(id){};

const std::string PhysicalWindowSinkOperator::toString() const {
    return "PhysicalWindowSinkOperator";
}

OperatorNodePtr PhysicalWindowSinkOperator::copy() {
    return create(id, windowDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES