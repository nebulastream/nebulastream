#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSCANOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSCANOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Scan operator.
 */
class PhysicalScanOperator : public PhysicalUnaryOperator, public AbstractScanOperator{
  public:
    PhysicalScanOperator(OperatorId id, SchemaPtr outputSchema);
    static PhysicalOperatorPtr create(OperatorId id, SchemaPtr outputSchema);
    static PhysicalOperatorPtr create(SchemaPtr outputSchema);
    const std::string toString() const override;
    OperatorNodePtr copy() override;
};
}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSCANOPERATOR_HPP_
