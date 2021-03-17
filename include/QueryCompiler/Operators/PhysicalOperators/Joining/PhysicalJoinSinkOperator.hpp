#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINSINKOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINSINKOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>
#include <Operators/AbstractOperators/AbstractJoinOperator.hpp>
namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalJoinSinkOperator: public PhysicalBinaryOperator, public AbstractJoinOperator {
  public:
    static PhysicalOperatorPtr create(OperatorId id,
                                      Join::LogicalJoinDefinitionPtr joinDefinition);
    static PhysicalOperatorPtr create(Join::LogicalJoinDefinitionPtr joinDefinition);
    PhysicalJoinSinkOperator(OperatorId id, Join::LogicalJoinDefinitionPtr joinDefinition);
    const std::string toString() const override;
    OperatorNodePtr copy() override;
};
}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINSINKOPERATOR_HPP_
