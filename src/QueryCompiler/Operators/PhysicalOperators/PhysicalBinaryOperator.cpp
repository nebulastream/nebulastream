


#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

PhysicalBinaryOperator::
PhysicalBinaryOperator(OperatorId id):
    OperatorNode(id), PhysicalOperator(id), BinaryOperatorNode(id) {}

}
}
}