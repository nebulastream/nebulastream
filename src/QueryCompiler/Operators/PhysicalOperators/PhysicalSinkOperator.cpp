

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalSinkOperator::PhysicalSinkOperator(OperatorId id) : OperatorNode(id), PhysicalUnaryOperator(id) {}

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

