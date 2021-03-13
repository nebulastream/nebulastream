

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

PhysicalUnaryOperator::
PhysicalUnaryOperator(OperatorId id):
    OperatorNode(id), PhysicalOperator(id), UnaryOperatorNode(id) {}

}
}
}