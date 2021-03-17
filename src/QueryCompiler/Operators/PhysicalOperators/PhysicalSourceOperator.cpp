

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalSourceOperator::PhysicalSourceOperator(OperatorId id, SourceDescriptorPtr sourceDescriptor)
    : OperatorNode(id), PhysicalUnaryOperator(id), sourceDescriptor(sourceDescriptor) {}

PhysicalOperatorPtr PhysicalSourceOperator::create(OperatorId id, SourceDescriptorPtr sourceDescriptor) {
    return std::make_shared<PhysicalSourceOperator>(id, sourceDescriptor);
}

PhysicalOperatorPtr PhysicalSourceOperator::create(SourceDescriptorPtr sourceDescriptor) {
    return create(UtilityFunctions::getNextOperatorId(), sourceDescriptor);
}

const std::string PhysicalSourceOperator::toString() const { return "PhysicalSourceOperator"; }

OperatorNodePtr PhysicalSourceOperator::copy() { return create(id, sourceDescriptor); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES