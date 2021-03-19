#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalWatermarkAssignmentOperator::PhysicalWatermarkAssignmentOperator(
    OperatorId id, Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor)
    : OperatorNode(id), AbstractWatermarkAssignerOperator(watermarkStrategyDescriptor), PhysicalUnaryOperator(id) {}
PhysicalOperatorPtr
PhysicalWatermarkAssignmentOperator::create(OperatorId id,
                                            Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor) {
    return std::make_shared<PhysicalWatermarkAssignmentOperator>(id, watermarkStrategyDescriptor);
}

PhysicalOperatorPtr
PhysicalWatermarkAssignmentOperator::create(Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor) {
    return create(UtilityFunctions::getNextOperatorId(), watermarkStrategyDescriptor);
}

const std::string PhysicalWatermarkAssignmentOperator::toString() const { return "PhysicalWatermarkAssignmentOperator"; }

OperatorNodePtr PhysicalWatermarkAssignmentOperator::copy() { return create(id, getWatermarkStrategyDescriptor()); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES