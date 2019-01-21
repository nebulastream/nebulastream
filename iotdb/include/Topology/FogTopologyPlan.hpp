#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_

#include <vector>
#include <bits/stdc++.h>
#include <algorithm>
#include "../Topology/FogToplogyLink.hpp"
#include "../Topology/FogToplogyNode.hpp"
#include "../Topology/FogToplogySensor.hpp"

#define MAX_NUMBER_OF_NODES 100 //TODO: make this dynamic

/**
 * TODOs:
 * 		- move functions to impl
 */


using namespace std;
class Graph
{
public:
    Graph(int pNumberOfElements)
	{
    	numberOfElements = pNumberOfElements;
		matrix = new FogTopologyLinkPtr*[numberOfElements];

		for (int i=0; i < numberOfElements; i++)
		{
		   matrix[i] = new FogTopologyLinkPtr[numberOfElements];

		   memset(matrix[i], 0, numberOfElements*sizeof(char));//the same as nullptr?
		}
	}

    // function to add an edge to graph
    void addEdge(size_t nodeID1, size_t nodeID2, FogTopologyLinkPtr ptr)
    {
    	//TODO: what to do if already exist?
    	matrix[nodeID1][nodeID2] = ptr;
    }

    FogTopologyLinkPtr getEdge(size_t nodeID1, size_t nodeID2)
    {
    	return matrix[nodeID1][nodeID2];
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
    FogTopologyLinkPtr **matrix;
};

class FogTopologyPlan{

public:
	FogTopologyPlan()
	{
		linkGraph = new Graph(MAX_NUMBER_OF_NODES);
	}

	void addFogNode(FogToplogyNodePtr ptr)
	{
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
		fogSensors.push_back(ptr);

	}
	void removeFogSensor(FogToplogyNodePtr ptr)
	{
		size_t search_id = ptr->getNodeId();

		for(std::vector<FogToplogySensorPtr>::iterator it = fogSensors.begin(); it != fogSensors.end(); ++it)
		{
			if(it->get()->getNodeId() == search_id)
				fogSensors.erase(it);
		}
	}
	void listFogSensors()
	{
		cout << "fogSensors:";
		for (auto& it : fogSensors)
		{
			cout << it->getNodeId() << ",";
		}
		cout << endl;
	}


private:
	//Basic assumption: Sensors are only connected to one node

	std::vector<FogToplogyNodePtr> fogNodes;

	std::vector<FogToplogySensorPtr> fogSensors;

	std::vector<FogTopologyLinkPtr> fogLinks;

	Graph* linkGraph;
};




#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
