#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYLINK_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYLINK_HPP_

#include <memory>

#include "FogTopologyEntry.hpp"
#include "Util/ErrorHandling.hpp"

namespace iotdb {
#define NOT_EXISTING_LINK_ID 0
#define INVALID_NODE_ID 101

enum LinkType { NodeToNode, NodeToSensor, SensorToNode };

static size_t linkID = 1;

class FogTopologyLink {

  public:
    FogTopologyLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode)
    {
        linkId = linkID++;
        sourceNode = pSourceNode;
        destNode = pDestNode;
    }

    void setLinkID(size_t pLinkID) { linkId = pLinkID; }

    size_t getId() { return linkId; }

    FogTopologyEntryPtr getSourceNode() { return sourceNode; }

    size_t getSourceNodeId() { return sourceNode->getId(); }

    FogTopologyEntryPtr getDestNode() { return destNode; }

    size_t getDestNodeId() { return destNode->getId(); }

    LinkType getLinkType()
    {
        if (sourceNode->getEntryType() == Worker && destNode->getEntryType() == Worker) {
            return NodeToNode;
        }
        else if (sourceNode->getEntryType() == Sensor && destNode->getEntryType() == Worker) {
            return SensorToNode;
        }
        else if (sourceNode->getEntryType() == Worker && destNode->getEntryType() == Sensor) {
            return NodeToSensor;
        }
        IOTDB_FATAL_ERROR("Unrecognized LinkType!");
    }

    std::string getLinkTypeString()
    {
        switch (getLinkType()) {
        case NodeToNode:
            return "NodeToNode";
        case SensorToNode:
            return "SensorToNode";
        case NodeToSensor:
            return "NodeToSensor";
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
