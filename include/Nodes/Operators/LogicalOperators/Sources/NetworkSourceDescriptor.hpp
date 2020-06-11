#ifndef NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_NETWORKSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_NETWORKSOURCEDESCRIPTOR_HPP_

#include <string>
#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Network/NetworkCommon.hpp>

namespace NES {
namespace Network {

/**
 * @brief The source descriptor for the NetworkSource
 */
class NetworkSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(SchemaPtr schema, NesPartition nesPartition);

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

    NesPartition getNesPartition() const;
  private:
    explicit NetworkSourceDescriptor(SchemaPtr schema, NesPartition nesPartition);

    SchemaPtr schema;
    NesPartition nesPartition;
};

typedef std::shared_ptr<NetworkSourceDescriptor> networkSourceDescriptor;

}
}

#endif //NES_INCLUDE_NODES_OPERATORS_LOGICALOPERATORS_SOURCES_NETWORKSOURCEDESCRIPTOR_HPP_
