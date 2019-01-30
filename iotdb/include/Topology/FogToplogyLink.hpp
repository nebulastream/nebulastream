#ifndef INCLUDE_TOPOLOGY_FOGTOPLOGYLINK_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPLOGYLINK_HPP_

#include <memory>

#define NOT_EXISTING_LINK_ID 0
#define INVALID_NODE_ID 101

enum LinkType { NodeToNode, NodeToSensor, SensorToNode};
enum NodeType { Worker, Sensor};


static size_t currentLinkID = 1;

class FogTopologyLink
{

public:
	FogTopologyLink(size_t pSourceNodeID, size_t pDestNodeID, LinkType type)
	{
		linkId = currentLinkID++;

		if(type == NodeToNode)
		{
			sourceNodeType = Worker;
			destNodeType = Worker;
		}
		else if (type == NodeToSensor)
		{
			sourceNodeType = Worker;
			destNodeType = Sensor;
		}
		else // SensorToNode
		{
			sourceNodeType = Sensor;
			destNodeType = Sensor;
		}

		sourceNodeID = pSourceNodeID;
		destNodeID = pDestNodeID;
	}

	void setLinkID(size_t pLinkID)
	{
		linkId = pLinkID;
	}

	size_t getLinkID()
	{
		return linkId;
	}

	size_t getSourceNodeID()
	{
		return sourceNodeID;
	}

	size_t getDestNodeID()
	{
		return destNodeID;
	}

private:
	size_t linkId;
	size_t sourceNodeID;
	size_t destNodeID;

	NodeType sourceNodeType;
	NodeType destNodeType;
};


typedef std::shared_ptr<FogTopologyLink> FogTopologyLinkPtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPLOGYLINK_HPP_ */
