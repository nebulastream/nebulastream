#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalJoinBuildOperator::create(Join::LogicalJoinDefinitionPtr joinDefinition) {
    return create(UtilityFunctions::getNextOperatorId(), joinDefinition);
}

PhysicalOperatorPtr PhysicalJoinBuildOperator::create(OperatorId id, Join::LogicalJoinDefinitionPtr joinDefinition) {
    return std::make_shared<PhysicalJoinBuildOperator>(id, joinDefinition);
}

PhysicalJoinBuildOperator::PhysicalJoinBuildOperator(OperatorId id, Join::LogicalJoinDefinitionPtr joinDefinition)
    : OperatorNode(id), PhysicalUnaryOperator(id), AbstractJoinOperator(joinDefinition) {};

const std::string PhysicalJoinBuildOperator::toString() const {
    return "PhysicalJoinBuildOperator";
}

OperatorNodePtr PhysicalJoinBuildOperator::copy() {
    return create(id, joinDefinition);
}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES