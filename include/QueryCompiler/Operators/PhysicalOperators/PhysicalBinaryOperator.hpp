#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALBINARYOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALBINARYOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <Operators/AbstractOperators/Arity/BinaryOperatorNode.hpp>
namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalBinaryOperator: public PhysicalOperator, public BinaryOperatorNode {
  protected:
    PhysicalBinaryOperator(OperatorId id);
};

}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALBINARYOPERATOR_HPP_
