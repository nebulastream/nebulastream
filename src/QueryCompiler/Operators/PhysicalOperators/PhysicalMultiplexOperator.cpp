

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMultiplexOperator.hpp>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalMultiplexOperator::create(OperatorId id) {
    return std::make_shared<PhysicalMultiplexOperator>(id);
}

PhysicalMultiplexOperator::PhysicalMultiplexOperator(OperatorId id) : OperatorNode(id), PhysicalUnaryOperator(id) {}

const std::string PhysicalMultiplexOperator::toString() const {
    return "PhysicalMultiplexOperator";
}
OperatorNodePtr PhysicalMultiplexOperator::copy() { return create(id); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES