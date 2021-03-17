#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalJoinSinkOperator::create(Join::LogicalJoinDefinitionPtr joinDefinition) {
    return create(UtilityFunctions::getNextOperatorId(), joinDefinition);
}

PhysicalOperatorPtr PhysicalJoinSinkOperator::create(OperatorId id, Join::LogicalJoinDefinitionPtr joinDefinition) {
    return std::make_shared<PhysicalJoinSinkOperator>(id, joinDefinition);
}

PhysicalJoinSinkOperator::PhysicalJoinSinkOperator(OperatorId id, Join::LogicalJoinDefinitionPtr joinDefinition)
    : OperatorNode(id), PhysicalBinaryOperator(id), AbstractJoinOperator(joinDefinition) {};

const std::string PhysicalJoinSinkOperator::toString() const {
    return "PhysicalJoinSinkOperator";
}

OperatorNodePtr PhysicalJoinSinkOperator::copy() {
    return create(id, joinDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES