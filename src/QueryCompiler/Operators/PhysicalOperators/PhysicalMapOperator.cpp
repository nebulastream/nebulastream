

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

PhysicalMapOperator::
PhysicalMapOperator(OperatorId id, FieldAssignmentExpressionNodePtr mapExpression):
    OperatorNode(id), PhysicalUnaryOperator(id), AbstractMapOperator(mapExpression) {}

}
}
}