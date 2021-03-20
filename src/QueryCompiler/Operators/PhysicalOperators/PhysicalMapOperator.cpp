

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

PhysicalMapOperator::
PhysicalMapOperator(OperatorId id, FieldAssignmentExpressionNodePtr mapExpression):
    OperatorNode(id), PhysicalUnaryOperator(id), AbstractMapOperator(mapExpression) {}

PhysicalOperatorPtr PhysicalMapOperator::create(OperatorId id, FieldAssignmentExpressionNodePtr mapExpression) {
    return std::make_shared<PhysicalMapOperator>(id, mapExpression);
}

PhysicalOperatorPtr PhysicalMapOperator::create(FieldAssignmentExpressionNodePtr mapExpression) {
    return create(UtilityFunctions::getNextOperatorId(), mapExpression);
}

const std::string PhysicalMapOperator::toString() const { return "PhysicalMapOperator"; }

OperatorNodePtr PhysicalMapOperator::copy() { return create(id, getMapExpression()); }

}
}
}