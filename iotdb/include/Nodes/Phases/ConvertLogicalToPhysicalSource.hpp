#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <SourceSink/DataSource.hpp>

namespace NES {

class ConvertLogicalToPhysicalSource {

  public:
    static DataSourcePtr createDataSource(SourceDescriptorPtr sourceDescriptor);

  private:
    ConvertLogicalToPhysicalSource() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
