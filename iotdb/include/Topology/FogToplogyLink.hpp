#ifndef INCLUDE_TOPOLOGY_FOGTOPLOGYLINK_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPLOGYLINK_HPP_

#include "FogTopologySensorProperties.hpp"
#define NOT_EXISTING_LINK_ID 0

class FogTopologyLink
{
public:
	FogTopologyLink(FogTopologySensorPropertiesPtr pPtr, size_t pId, size_t pSourceNodeID, size_t pDestNodeID)
	{
		ptr = pPtr;
		assert(pId != NOT_EXISTING_LINK_ID);
		linkID = pId;
		sourceNodeID = pSourceNodeID;
		destNodeID = pDestNodeID;

	}
	size_t getLinkID()
	{
		return linkID;
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
	FogTopologySensorPropertiesPtr ptr;
	size_t linkID;
	size_t sourceNodeID;
	size_t destNodeID;

};


typedef std::shared_ptr<FogTopologyLink> FogTopologyLinkPtr;

#endif /* INCLUDE_TOPOLOGY_FOGTOPLOGYLINK_HPP_ */
