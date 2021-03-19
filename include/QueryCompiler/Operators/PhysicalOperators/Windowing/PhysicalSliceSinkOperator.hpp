#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICESINKOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICESINKOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <Operators/AbstractOperators/AbstractWindowOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

class PhysicalSliceSinkOperator : public AbstractWindowOperator, public PhysicalUnaryOperator {
  public:
    PhysicalSliceSinkOperator(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition);
    static PhysicalOperatorPtr create(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition);
    static PhysicalOperatorPtr create(Windowing::LogicalWindowDefinitionPtr windowDefinition);
    const std::string toString() const override;
    OperatorNodePtr copy() override;

};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALSLICESINKOPERATOR_HPP_
