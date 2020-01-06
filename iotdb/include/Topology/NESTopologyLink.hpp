#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_

#include <memory>

#include "NESTopologyEntry.hpp"

namespace iotdb {
#define NOT_EXISTING_LINK_ID std::numeric_limits<size_t>::max()

enum LinkType { NodeToNode, NodeToSensor, SensorToNode };

//FIXME: add docu here
class NESTopologyLink {

 public:
  explicit NESTopologyLink(size_t pLinkId, NESTopologyEntryPtr pSourceNode, NESTopologyEntryPtr pDestNode) {
    linkId = pLinkId;
    sourceNode = pSourceNode;
    destNode = pDestNode;
  }

  size_t getId() { return linkId; }

  NESTopologyEntryPtr getSourceNode() { return sourceNode; }

  size_t getSourceNodeId() { return sourceNode->getId(); }

  NESTopologyEntryPtr getDestNode() { return destNode; }

  size_t getDestNodeId() { return destNode->getId(); }

  LinkType getLinkType();

  std::string getLinkTypeString();

 private:
  size_t linkId;

  NESTopologyEntryPtr sourceNode;
  NESTopologyEntryPtr destNode;
};

typedef std::shared_ptr<NESTopologyLink> NESTopologyLinkPtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYLINK_HPP_ */
