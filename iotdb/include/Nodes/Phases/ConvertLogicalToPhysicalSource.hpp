#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_

#include <SourceSink/DataSource.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

class ConvertLogicalToPhysicalSource {

  public:
    static DataSourcePtr createDataSource(SourceDescriptorPtr sourceDescriptor);

  private:
    ConvertLogicalToPhysicalSource() = default;
};

}


#endif //NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
