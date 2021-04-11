#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALEMITOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALEMITOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Emit operator.
 */
class PhysicalEmitOperator : public PhysicalUnaryOperator {
  public:
    PhysicalEmitOperator(OperatorId id, SchemaPtr inputSchema);
    static PhysicalOperatorPtr create(OperatorId id, SchemaPtr inputSchema);
    static PhysicalOperatorPtr create(SchemaPtr inputSchema);
    const std::string toString() const override;
    OperatorNodePtr copy() override;
};
}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALEMITOPERATOR_HPP_
