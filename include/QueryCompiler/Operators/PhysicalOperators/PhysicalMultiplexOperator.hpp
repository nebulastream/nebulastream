#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMULTIPLEXOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMULTIPLEXOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalMultiplexOperator : public PhysicalUnaryOperator {
  public:
    PhysicalMultiplexOperator(OperatorId id);
    static PhysicalOperatorPtr create(OperatorId id = UtilityFunctions::getNextOperatorId());
    const std::string toString() const override;
    OperatorNodePtr copy() override;

};

}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMULTIPLEXOPERATOR_HPP_
