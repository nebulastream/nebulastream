#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYLINK_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYLINK_HPP_

#include <memory>
#include "FogTopologyEntry.hpp"
#define NOT_EXISTING_LINK_ID 0
#define INVALID_NODE_ID 101

enum LinkType { NodeToNode, NodeToSensor, SensorToNode };
//enum NodeType { Worker, Sensor };

static size_t currentLinkID = 1;

class FogTopologyLink {

public:
  FogTopologyLink(size_t pSourceNodeId, size_t pDestNodeId) {
    linkId = currentLinkID++;

//    if (type == NodeToNode) {
//      sourceNodeType = Worker;
//      destNodeType = Worker;
//    } else if (type == NodeToSensor) {
//      sourceNodeType = Worker;
//      destNodeType = Sensor;
//    } else // SensorToNode
//    {
//      sourceNodeType = Sensor;
//      destNodeType = Sensor;
//    }

    sourceNodeID = pSourceNodeId;
    destNodeID = pDestNodeId;
  }

  void setLinkID(size_t pLinkID) { linkId = pLinkID; }

  size_t getId() { return linkId; }

  size_t getSourceNodeId() { return sourceNodeID; }

  size_t getDestNodeId() { return destNodeID; }

  LinkType getLinkType() {
    if (sourceNodeType.getEntryType() == Worker && destNodeType.getEntryType() == Worker) {
        return NodeToNode;
    } else if (sourceNodeType.getEntryType() == Sensor && destNodeType.getEntryType() == Worker) {
        return SensorToNode;
    } else if (sourceNodeType.getEntryType() == Worker && destNodeType.getEntryType() == Sensor) {
        return NodeToSensor;
    }
    IOTDB_FATAL_ERROR("Unrecognized LinkType!");
  }

  std::string getLinkTypeString() {
      switch(getLinkType) {
          case NodeToNode: return "NodeToNode";
          case SensorToNode: return "SensorToNode";
          case NodeToSensor: return "NodeToSensor";
      }
      IOTDB_FATAL_ERROR("String for LinkType not found!");
  }

private:
  size_t linkId;
  size_t sourceNodeID;
  size_t destNodeID;

  FogTopologyEntryPtr sourceNode;
  FogTopologyEntryPtr destNode;
};

typedef std::shared_ptr<FogTopologyLink> FogTopologyLinkPtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYLINK_HPP_ */
