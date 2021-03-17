#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_FILTERPHYSICALOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_FILTERPHYSICALOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <Operators/AbstractOperators/AbstractFilterOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalFilterOperator: public PhysicalUnaryOperator, public AbstractFilterOperator{
  public:
    PhysicalFilterOperator(OperatorId id, ExpressionNodePtr predicate);
    static PhysicalOperatorPtr create(OperatorId id, ExpressionNodePtr predicate);
    static PhysicalOperatorPtr create(ExpressionNodePtr predicate);
    const std::string toString() const override;
    OperatorNodePtr copy() override;

};
}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_FILTERPHYSICALOPERATOR_HPP_
