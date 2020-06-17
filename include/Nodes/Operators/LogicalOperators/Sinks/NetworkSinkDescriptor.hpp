#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_NETWORKSINKDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_NETWORKSINKDESCRIPTOR_HPP_

#include <Network/NetworkCommon.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <string>

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
                                    uint32_t retryTimes);

    /**
     * @brief returns the string representation of the network sink
     * @return the string representation
     */
    std::string toString() override;

    /**
     * @brief equal method for the NetworkSinkDescriptor
     * @param other
     * @return true if equal, else false
     */
    bool equal(SinkDescriptorPtr other) override;

    /**
     * @brief getter for the node location
     * @return the node location
     */
    NodeLocation getNodeLocation() const;

    /**
     * @brief getter for the nes partition
     * @return the nes partition
     */
    NesPartition getNesPartition() const;

    /**
     * @brief getter for the wait time
     * @return the wait time
     */
    std::chrono::seconds getWaitTime() const;

    /**
     * @brief getter for the retry times
     * @return the retry times
     */
    uint8_t getRetryTimes() const;

  private:
    explicit NetworkSinkDescriptor(NodeLocation nodeLocation, NesPartition nesPartition,
                                   std::chrono::seconds waitTime, uint32_t retryTimes = 5);

    NodeLocation nodeLocation;
    NesPartition nesPartition;
    std::chrono::seconds waitTime;
    uint32_t retryTimes;
};

typedef std::shared_ptr<NetworkSinkDescriptor> NetworkSinkDescriptorPtr;

}// namespace Network
}// namespace NES

#endif//NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SINKS_NETWORKSINKDESCRIPTOR_HPP_
