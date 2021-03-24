#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical operator for all window sinks.
 * A window sink computes the final window result using the window slice store.
 */
class PhysicalWindowOperator : public PhysicalUnaryOperator {
  public:
    PhysicalWindowOperator(OperatorId id, Windowing::LogicalWindowDefinitionPtr windowDefinition);

    /**
    * @brief Gets the window definition of the window operator.
    * @return LogicalWindowDefinitionPtr
    */
    Windowing::LogicalWindowDefinitionPtr getWindowDefinition() const;
  protected:

    Windowing::LogicalWindowDefinitionPtr windowDefinition;

};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_PHYSICALWINDOWOPERATOR_HPP_
