#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTLOGICALTOPHYSICALSINK_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTLOGICALTOPHYSICALSINK_HPP_

#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <SourceSink/DataSink.hpp>

namespace NES {

class ConvertLogicalToPhysicalSink {

  public:
    static DataSinkPtr createDataSink(SinkDescriptorPtr sinkDescriptor);

  private:
    ConvertLogicalToPhysicalSink() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTLOGICALTOPHYSICALSINK_HPP_
