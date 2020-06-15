#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_NETWORKSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_NETWORKSOURCEDESCRIPTOR_HPP_

#include <string>
#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Network/NetworkCommon.hpp>

namespace NES {
namespace Network {

/**
 * @brief Descriptor defining properties used for creating physical zmq source inside the network stack
 */
class NetworkSourceDescriptor : public SourceDescriptor {

  public:
    /**
     * @brief The constructor for the network source descriptor
     * @param schema
     * @param nesPartition
     * @return instance of network source descriptor
     */
    static SourceDescriptorPtr create(SchemaPtr schema, NesPartition nesPartition);

    /**
     * @brief equal method for the NetworkSourceDescriptor
     * @param other
     * @return true if equal, else false
     */
    bool equal(SourceDescriptorPtr other) override;

    /**
     * @brief creates a string representation of the NetworkSourceDescriptor
     * @return the string representation
     */
    std::string toString() override;

    /**
     * @brief getter for the nes partition
     * @return the nesPartition
     */
    NesPartition getNesPartition() const;
  private:
    explicit NetworkSourceDescriptor(SchemaPtr schema, NesPartition nesPartition);

    SchemaPtr schema;
    NesPartition nesPartition;
};

typedef std::shared_ptr<NetworkSourceDescriptor> networkSourceDescriptorPtr;

}
}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_NETWORKSOURCEDESCRIPTOR_HPP_
