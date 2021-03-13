#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALUNARYOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALUNARYOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalUnaryOperator: public PhysicalOperator, public UnaryOperatorNode {
  protected:
    PhysicalUnaryOperator(OperatorId id);
};

}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATOR_HPP_
