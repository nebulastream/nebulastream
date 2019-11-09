#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYLINK_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYLINK_HPP_

#include <memory>

#include "FogTopologyEntry.hpp"
#include "Util/ErrorHandling.hpp"

namespace iotdb {
#define NOT_EXISTING_LINK_ID std::numeric_limits<size_t>::max()

enum LinkType { NodeToNode, NodeToSensor, SensorToNode };

class FogTopologyLink {

 public:
  explicit FogTopologyLink(size_t pLinkId, FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode) {
    linkId = pLinkId;
    sourceNode = pSourceNode;
    destNode = pDestNode;
  }

  size_t getId() { return linkId; }

  FogTopologyEntryPtr getSourceNode() { return sourceNode; }

  size_t getSourceNodeId() { return sourceNode->getId(); }

  FogTopologyEntryPtr getDestNode() { return destNode; }

  size_t getDestNodeId() { return destNode->getId(); }

  LinkType getLinkType() {
    if (sourceNode->getEntryType() == Worker && destNode->getEntryType() == Worker) {
      return NodeToNode;
    } else if (sourceNode->getEntryType() == Sensor && destNode->getEntryType() == Worker) {
      return SensorToNode;
    } else if (sourceNode->getEntryType() == Worker && destNode->getEntryType() == Sensor) {
      return NodeToSensor;
    }
    IOTDB_FATAL_ERROR("Unrecognized LinkType!");
  }

  std::string getLinkTypeString() {
    switch (getLinkType()) {
      case NodeToNode:return "NodeToNode";
      case SensorToNode:return "SensorToNode";
      case NodeToSensor:return "NodeToSensor";
    }
    IOTDB_FATAL_ERROR("String for LinkType not found!");
  }

 private:
  size_t linkId;

  FogTopologyEntryPtr sourceNode;
  FogTopologyEntryPtr destNode;
};

typedef std::shared_ptr<FogTopologyLink> FogTopologyLinkPtr;
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYLINK_HPP_ */
