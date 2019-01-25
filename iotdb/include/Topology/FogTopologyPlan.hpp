#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_

#include <vector>
#include <bits/stdc++.h>
#include <algorithm>
#include "../Topology/FogToplogyLink.hpp"
#include "../Topology/FogToplogyNode.hpp"
#include "../Topology/FogToplogySensor.hpp"

#define MAX_NUMBER_OF_NODES 10 //TODO: make this dynamic
/**
 * TODOs:
 * 		- move functions to impl
 * 	Assumptions
 * 		-
 */


using namespace std;
class Graph
{
public:
    Graph(int pNumberOfElements)
	{
    	numberOfElements = pNumberOfElements;
		matrix = new size_t*[numberOfElements];

		for (int i=0; i < numberOfElements; i++)
		{
		   matrix[i] = new size_t[numberOfElements];

		   memset(matrix[i], NOT_EXISTING_LINK_ID, numberOfElements*sizeof(size_t));
		}
	}

    // function to add an edge to graph
    void addLink(size_t nodeID1, size_t nodeID2, size_t linkID)
    {
    	//TODO: what to do if already exist?
    	matrix[nodeID1][nodeID2] = linkID;
    }

    size_t getLinkID(size_t nodeID1, size_t nodeID2)
    {
    	return matrix[nodeID1][nodeID2];
    }

    void removeLinkID(size_t nodeID1, size_t nodeID2)
	{
		matrix[nodeID1][nodeID2] = NOT_EXISTING_LINK_ID;
	}

    void print()
    {
		for (int u = 0; u < numberOfElements; u++)
		{
		  for (int v = 0; v < numberOfElements; v++)
			 std::cout << matrix[u][v] << " ";
		  std::cout << std::endl;
		}
    }
private:
    int numberOfElements;    // No. of vertices
    size_t **matrix;
};

class FogTopologyPlan{

public:
	FogTopologyPlan()
	{
		linkGraph = new Graph(MAX_NUMBER_OF_NODES);
		currentId = 1;
	}

	void addFogNode(FogToplogyNodePtr ptr)
	{
		ptr->setNodeId(currentId++);
		fogNodes.push_back(ptr);
	}
	void removeFogNode(FogToplogyNodePtr ptr)
	{
		size_t search_id = ptr->getNodeId();

		for(std::vector<FogToplogyNodePtr>::iterator it = fogNodes.begin(); it != fogNodes.end(); ++it)
		{
			if(it->get()->getNodeId() == search_id)
				fogNodes.erase(it);
		}

	}
	void listFogNodes()
	{
		cout << "fogNodes:";
		for (auto& it : fogNodes)
		{
			cout << it->getNodeId() << ",";
		}
		cout << endl;
	}

	void addFogSensor(FogToplogySensorPtr ptr)
	{
		ptr->setSensorID(currentId++);
		fogSensors.push_back(ptr);

	}
	void removeFogSensor(FogToplogyNodePtr ptr)
	{
		size_t search_id = ptr->getNodeId();

		for(std::vector<FogToplogySensorPtr>::iterator it = fogSensors.begin(); it != fogSensors.end(); ++it)
		{
			if(it->get()->getSensorId() == search_id)
				fogSensors.erase(it);
		}
	}
	void listFogSensors()
	{
		cout << "fogSensors:";
		for (auto& it : fogSensors)
		{
			cout << it->getSensorId()<< ",";
		}
		cout << endl;
	}

	/**
	 * Support half-duplex links?
	 */
	void addFogTopologyLink(size_t pSourceNodeID, size_t pDestNodeID, LinkType type)
	{
		FogTopologyLinkPtr linkPtr = std::make_shared<FogTopologyLink>(pSourceNodeID, pDestNodeID, type);
		linkPtr->setLinkID(currentId++);

		fogLinks.push_back(linkPtr);
		linkGraph->addLink(linkPtr->getSourceNodeID(), linkPtr->getDestNodeID(), linkPtr->getLinkID());
		linkGraph->addLink(linkPtr->getDestNodeID(), linkPtr->getSourceNodeID(), linkPtr->getLinkID());
	}
	void removeFogTopologyLink(FogTopologyLinkPtr linkPtr)
	{
		size_t search_id = linkPtr->getLinkID();

		for(std::vector<FogTopologyLinkPtr>::iterator it = fogLinks.begin(); it != fogLinks.end(); ++it)
		{
			if(it->get()->getLinkID() == search_id)
			{
				linkGraph->removeLinkID(linkPtr->getSourceNodeID(), linkPtr->getDestNodeID());
				fogLinks.erase(it);
			}
		}
	}

	void printPlan()
	{
		linkGraph->print();
	}
private:

	std::vector<FogToplogyNodePtr> fogNodes;
	std::vector<FogToplogySensorPtr> fogSensors;
	std::vector<FogTopologyLinkPtr> fogLinks;
	size_t currentId;
	Graph* linkGraph;
};




#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
