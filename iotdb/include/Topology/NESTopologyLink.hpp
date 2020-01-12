#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_

#include <memory>

#include "NESTopologyEntry.hpp"

namespace iotdb {
#define NOT_EXISTING_LINK_ID std::numeric_limits<size_t>::max()

enum LinkType {
  NodeToNode,
  NodeToSensor,
  SensorToNode
};

//FIXME: add docu here
class NESTopologyLink {

 public:
  explicit NESTopologyLink(size_t pLinkId, NESTopologyEntryPtr pSourceNode,
                           NESTopologyEntryPtr pDestNode) {
    linkId = pLinkId;
    sourceNode = pSourceNode;
    destNode = pDestNode;
  }

  /**
   * @brief method to get the id of the node
   * @return id as a size_t
   */
  size_t getId();

  /**
   * @brief get the source node of a link
   * @return NESTopologyEntryPtr to the source node
   */
  NESTopologyEntryPtr getSourceNode();

  /**
   * @brief method to get the id of the source of a lin
   * @return size_t id of the source node
   */
  size_t getSourceNodeId();

  /**
   * @brief get the destination node of a link
   * @return NESTopologyEntryPtr to the destination node
   */
  NESTopologyEntryPtr getDestNode();

  /**
   * @brief get the id of the destination node of the link
   * @return size_t of destination node
   */
  size_t getDestNodeId();

  /**
   * @brief get the type of the link
   * @return LinkType
   */
  LinkType getLinkType();

  /**
   * @brief get the type of the link as a string
   * @return link type as string
   */
  std::string getLinkTypeString();

 private:
  size_t linkId;

  NESTopologyEntryPtr sourceNode;
  NESTopologyEntryPtr destNode;
};

typedef std::shared_ptr<NESTopologyLink> NESTopologyLinkPtr;
}  // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_ */
