#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSOURCEOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSOURCEOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalSourceOperator: public PhysicalUnaryOperator {
  public:
    PhysicalSourceOperator(OperatorId id, SourceDescriptorPtr sourceDescriptor);
    static PhysicalOperatorPtr create(OperatorId id, SourceDescriptorPtr sourceDescriptor);
    static PhysicalOperatorPtr create(SourceDescriptorPtr sourceDescriptor);
    const std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    SourceDescriptorPtr sourceDescriptor;

};
}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSOURCEOPERATOR_HPP_
