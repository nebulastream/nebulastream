

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalSourceOperator::PhysicalSourceOperator(OperatorId id) : OperatorNode(id), PhysicalUnaryOperator(id) {}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES