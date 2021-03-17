#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSINKOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSINKOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES{
namespace QueryCompilation{
namespace PhysicalOperators{

class PhysicalSinkOperator: public PhysicalUnaryOperator {
  public:
    PhysicalSinkOperator(OperatorId id, SinkDescriptorPtr sinkDescriptor);
    static PhysicalOperatorPtr create(OperatorId id, SinkDescriptorPtr sinkDescriptor);
    static PhysicalOperatorPtr create(SinkDescriptorPtr sinkDescriptor);
    const std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    SinkDescriptorPtr sinkDescriptor;

};
}
}
}

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSINKOPERATOR_HPP_
