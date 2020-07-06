#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTLOGICALTOPHYSICALSINK_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTLOGICALTOPHYSICALSINK_HPP_

#include "Network/NetworkSink.hpp"
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class NodeEngine;
typedef std::shared_ptr<NodeEngine> NodeEnginePtr;

/**
 * @brief This class is responsible for creating the physical sink from Logical sink description
 */
class ConvertLogicalToPhysicalSink {

  public:
    /**
     * @brief This method is responsible for creating the physical sink from logical sink descriptor
     * @param sinkDescriptor: logical sink descriptor
     * @param nodeEngine: the running node engine where the sink is deployed
     * @return Data sink pointer representing the physical sink
     */
    static DataSinkPtr createDataSink(SchemaPtr schema, SinkDescriptorPtr sinkDescriptor, NodeEnginePtr nodeEngine);

  private:
    ConvertLogicalToPhysicalSink() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTLOGICALTOPHYSICALSINK_HPP_
