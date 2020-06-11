#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_NETWORKSINKDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_NETWORKSINKDESCRIPTOR_HPP_

#include <string>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Network/NetworkCommon.hpp>
#include <QueryCompiler/CodeGenerator.hpp>

namespace NES {
namespace Network {

/**
 * @brief Descriptor defining properties used for creating physical zmq sink
 */
class NetworkSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief The constructor for the network sink descriptor
     * @param nodeLocation
     * @param nesPartition
     * @param waitTime
     * @param retryTimes
     * @return SinkDescriptorPtr
     */
    static SinkDescriptorPtr create(NodeLocation nodeLocation,
                                    NesPartition nesPartition,
                                    std::chrono::seconds waitTime,
                                    uint8_t retryTimes);

    std::string toString() override;

    bool equal(SinkDescriptorPtr other) override;

    NodeLocation getNodeLocation() const;
    NesPartition getNesPartition() const;
    std::chrono::seconds getWaitTime() const;
    uint8_t getRetryTimes() const;

  private:
    explicit NetworkSinkDescriptor(NodeLocation nodeLocation, NesPartition nesPartition,
                                   std::chrono::seconds waitTime, uint8_t retryTimes = 5);

    NodeLocation nodeLocation;
    NesPartition nesPartition;
    std::chrono::seconds waitTime;
    uint8_t retryTimes;
};

typedef std::shared_ptr<NetworkSinkDescriptor> NetworkSinkDescriptorPtr;

}
}// namespace NES


#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_NETWORKSINKDESCRIPTOR_HPP_
