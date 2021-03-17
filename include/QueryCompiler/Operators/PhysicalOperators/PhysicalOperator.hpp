#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATOR_HPP_
#include <Operators/OperatorNode.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalOperator: public virtual OperatorNode {
  protected:
    PhysicalOperator(OperatorId id);
};

}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALOPERATOR_HPP_
