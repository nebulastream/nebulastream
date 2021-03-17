

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalDemultiplexOperator.hpp>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalOperatorPtr PhysicalDemultiplexOperator::create(OperatorId id) {
    return std::make_shared<PhysicalDemultiplexOperator>(id);
}

PhysicalDemultiplexOperator::PhysicalDemultiplexOperator(OperatorId id) : OperatorNode(id), PhysicalUnaryOperator(id) {}

const std::string PhysicalDemultiplexOperator::toString() const {
    return "PhysicalDemultiplexOperator";
}
OperatorNodePtr PhysicalDemultiplexOperator::copy() { return create(id); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES