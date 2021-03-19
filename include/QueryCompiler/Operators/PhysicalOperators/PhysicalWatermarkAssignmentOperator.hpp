#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALWATERMARKASSIGNMENTOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALWATERMARKASSIGNMENTOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <Operators/AbstractOperators/AbstractWatermarkAssignerOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalWatermarkAssignmentOperator: public AbstractWatermarkAssignerOperator, public PhysicalUnaryOperator {
  public:
    PhysicalWatermarkAssignmentOperator(OperatorId id, const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    static PhysicalOperatorPtr create(OperatorId id, const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    static PhysicalOperatorPtr create(Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    const std::string toString() const override;
    OperatorNodePtr copy() override;

};
}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALWATERMARKASSIGNMENTOPERATOR_HPP_
