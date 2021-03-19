#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICEMERGINGOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICEMERGINGOPERATOR_HPP_

#include <Operators/AbstractOperators/AbstractWindowOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

class PhysicalSliceMergingOperator : public AbstractWindowOperator, public PhysicalUnaryOperator {
  public:
    PhysicalSliceMergingOperator(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition);
    static PhysicalOperatorPtr create(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition);
    static PhysicalOperatorPtr create(Windowing::LogicalWindowDefinitionPtr windowDefinition);
    const std::string toString() const override;
    OperatorNodePtr copy() override;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICEMERGINGOPERATOR_HPP_
