#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
#include <Operators/AbstractOperators/AbstractMapOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalMapOperator: public PhysicalUnaryOperator, public AbstractMapOperator{
  public:
    PhysicalMapOperator(OperatorId id, FieldAssignmentExpressionNodePtr mapExpression);

};
}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALMAPOPERATOR_HPP_
