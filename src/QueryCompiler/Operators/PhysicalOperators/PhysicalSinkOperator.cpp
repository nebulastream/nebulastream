

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalSinkOperator::PhysicalSinkOperator(OperatorId id, SinkDescriptorPtr sinkDescriptor)
    : OperatorNode(id), PhysicalUnaryOperator(id), sinkDescriptor(sinkDescriptor) {}

PhysicalOperatorPtr PhysicalSinkOperator::create(SinkDescriptorPtr sinkDescriptor) {
    return create(UtilityFunctions::getNextOperatorId(), sinkDescriptor);
}

PhysicalOperatorPtr PhysicalSinkOperator::create(OperatorId id, SinkDescriptorPtr sinkDescriptor) {
    return std::make_shared<PhysicalSinkOperator>(id, sinkDescriptor);
}

const std::string PhysicalSinkOperator::toString() const { return "PhysicalSinkOperator"; }

OperatorNodePtr PhysicalSinkOperator::copy() { return create(id, sinkDescriptor); }


}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES
