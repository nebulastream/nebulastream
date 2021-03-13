
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

PhysicalFilterOperator::
    PhysicalFilterOperator(OperatorId id, ExpressionNodePtr predicate):
              OperatorNode(id), PhysicalUnaryOperator(id), AbstractFilterOperator(predicate) {}

}
}
}