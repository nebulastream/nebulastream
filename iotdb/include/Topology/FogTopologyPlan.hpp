#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_

#include <vector>
#include <bits/stdc++.h>
#include <algorithm>
#include "../Topology/FogToplogyLink.hpp"
#include "../Topology/FogToplogyNode.hpp"
#include "../Topology/FogToplogySensor.hpp"
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/io.hpp>

#define MAX_NUMBER_OF_NODES 10 //TODO: make this dynamic
/**
 * TODOs:
 * 		- move functions to impl
 * 	Assumptions
 * 		-
 */


using namespace std;
using namespace boost::numeric::ublas;

class Graph
{
public:
    Graph(int pNumberOfElements)
	{
    	numberOfRows = numberOfColums = 1;
    	mtx.resize(numberOfRows,numberOfColums);
    	mtx(0,0) = 0;
	}

    void addNode()
    {
    	numberOfRows++;
    	numberOfColums++;
    	mtx.resize(numberOfRows,numberOfColums);
    }
    // function to add an edge to graph
    void addLink(size_t nodeID1, size_t nodeID2, size_t linkID)
    {
    	//TODO: what to do if already exist?
    	mtx(nodeID1, nodeID2) = linkID;
    }

    size_t getLinkID(size_t nodeID1, size_t nodeID2)
    {
    	return mtx(nodeID1,nodeID2);
    }

    void removeLinkID(size_t nodeID1, size_t nodeID2)
	{
		mtx(nodeID1,nodeID2) = 0;
	}

    void print()
    {
    	//first line
    	cout << " | ";
    	for (size_t v = 0; v < numberOfRows; v++)
    	{
    		if(v == 0)
    			cout << "-" << " ";
    		else
    			cout << v << " ";
    	}
    	cout << endl;
    	for (size_t v = 0; v < numberOfColums*2+3; v++)
			cout << "-";
    	cout << endl;

		for (size_t u = 0; u < numberOfRows; u++)
		{
			if(u == 0)
				cout << "-| ";
			else
				cout << u << "| ";
		  for (size_t v = 0; v < numberOfColums; v++)
		  {
			  if(mtx(u,v) == 0)
				  std::cout << "- ";
			  else
				  std::cout << mtx(u,v) << " ";
		  }
		  std::cout << std::endl;
		}
    }

private:
    size_t numberOfRows;
    size_t numberOfColums;
    matrix<size_t> mtx;

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
		linkGraph->addNode();
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
		linkGraph->addNode();

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
		cout << "Nodes IDs=";
		for (size_t u = 0; u < fogNodes.size(); u++)
		{
			cout << fogNodes[u]->getNodeId();
			if(u != fogNodes.size()-1)
				cout << ",";
		}
		cout << endl;

		cout << "Sensors IDs=";
		for (size_t u = 0; u < fogSensors.size(); u++)
		{
			cout << fogSensors[u]->getSensorId();
			if(u != fogSensors.size()-1)
				cout << ",";
		}
		cout << endl;
	}
private:

	std::vector<FogToplogyNodePtr> fogNodes;
	std::vector<FogToplogySensorPtr> fogSensors;
	std::vector<FogTopologyLinkPtr> fogLinks;
	size_t currentId;
	Graph* linkGraph;
};




#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
