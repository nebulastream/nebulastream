#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_

#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Sources/DataSource.hpp>

namespace NES {

class NodeEngine;
typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

/**
 * @brief This class is responsible for creating logical source descriptor to physical source.
 */
class ConvertLogicalToPhysicalSource {

  public:
    /**
     * @brief This method produces corresponding physical source for an input logical source descriptor
     * @param sourceDescriptor : the logical source desciptor
     * @return Data source pointer for the physical source
     */
    static DataSourcePtr createDataSource(SourceDescriptorPtr sourceDescriptor, NodeEnginePtr nodeEngine);

  private:
    ConvertLogicalToPhysicalSource() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SINKS_CONVERTLOGICALTOPHYSICALSOURCE_HPP_
