#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalSlicePreAggregationOperator::create(Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return create(UtilityFunctions::getNextOperatorId(), windowDefinition);
}

PhysicalOperatorPtr PhysicalSlicePreAggregationOperator::create(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition) {
    return std::make_shared<PhysicalSlicePreAggregationOperator>(id, windowDefinition);
}

PhysicalSlicePreAggregationOperator::PhysicalSlicePreAggregationOperator(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition)
    : OperatorNode(id), AbstractWindowOperator(windowDefinition), PhysicalUnaryOperator(id){};

const std::string PhysicalSlicePreAggregationOperator::toString() const {
    return "PhysicalWindowPreAggregationOperator";
}

OperatorNodePtr PhysicalSlicePreAggregationOperator::copy() {
    return create(id, windowDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES