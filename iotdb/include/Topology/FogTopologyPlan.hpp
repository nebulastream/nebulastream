#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_

#include <map>
#include <memory>
#include <vector>
#include <algorithm>
#include <bits/stdc++.h>

#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/io.hpp>
#include <boost/numeric/ublas/matrix_proxy.hpp>

#include "Topology/FogToplogyLink.hpp"
#include "Topology/FogToplogyNode.hpp"
#include "Topology/FogToplogySensor.hpp"



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

//    void print()
//    {
//    	//first line
//    	cout << " | ";
//    	for (size_t v = 0; v < numberOfRows; v++)
//    	{
//    		if(v == 0)
//    			cout << "-" << " ";
//    		else
//    			cout << v << " ";
//    	}
//    	cout << endl;
//    	for (size_t v = 0; v < numberOfColums*2+3; v++)
//			cout << "-";
//    	cout << endl;
//
//		for (size_t u = 0; u < numberOfRows; u++)
//		{
//			if(u == 0)
//				cout << "-| ";
//			else
//				cout << u << "| ";
//		  for (size_t v = 0; v < numberOfColums; v++)
//		  {
//			  if(mtx(u,v) == 0)
//				  std::cout << "- ";
//			  else
//				  std::cout << mtx(u,v) << " ";
//		  }
//		  std::cout << std::endl;
//		}
//    }
    void print()
    {
    	print(mtx);
    }

    void print(matrix<size_t> pMtx)
	{
//    	size1_ == rows
//		size2_ == cols

		//first line
		cout << " | ";
		for (size_t v = 0; v < pMtx.size1(); v++)
		{
			if(v == 0)
				cout << "-" << " ";
			else
				cout << v << " ";
		}
		cout << endl;
		for (size_t v = 0; v < pMtx.size2()*2+3; v++)
			cout << "-";
		cout << endl;

		for (size_t u = 0; u < pMtx.size1(); u++)
		{
			if(u == 0)
				cout << "-| ";
			else
				cout << u << "| ";
		  for (size_t v = 0; v < pMtx.size2(); v++)
		  {
			  if(pMtx(u,v) == 0)
				  std::cout << "- ";
			  else
				  std::cout << pMtx(u,v) << " ";
		  }
		  std::cout << std::endl;
		}
	}

    void removeRowAndCol()
    {
        matrix_slice<matrix<size_t> > ms (mtx, boost::numeric::ublas::slice (0, 1, 3), boost::numeric::ublas::slice (0, 1, 3));
        print(ms);

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
//		if ( fogNodes.find("f") == m.end() ) {
//		  // not found
//		} else {
//		  // found
//		}
		//TODO: check if id exists
		fogNodes[currentId]= ptr;
		ptr->setNodeId(currentId);
		currentId++;
		linkGraph->addNode();
	}
	void removeFogNode(FogToplogyNodePtr ptr)
	{
		size_t search_id = ptr->getNodeId();
		fogNodes.erase(search_id);
	}
	void listFogNodes()
	{
		cout << "fogNodes:";
		for (auto const& it : fogNodes)
		{
			cout << it.second->getNodeId() << ",";
		}
		cout << endl;
	}

	void addFogSensor(FogToplogySensorPtr ptr)
	{
		fogSensors[currentId] = ptr;
		ptr->setSensorID(currentId);
		currentId++;
		linkGraph->addNode();

	}
	void removeFogSensor(FogToplogyNodePtr ptr)
	{
		size_t search_id = ptr->getNodeId();

		fogSensors.erase(search_id);
	}
	void listFogSensors()
	{
		cout << "fogSensors:";
		for (auto const& it : fogSensors)
		{
			cout << it.second->getSensorId() << ",";
		}
		cout << endl;
	}

	/**
	 * Support half-duplex links?
	 */
	void addFogTopologyLink(size_t pSourceNodeID, size_t pDestNodeID, LinkType type)
	{
		FogTopologyLinkPtr linkPtr = std::make_shared<FogTopologyLink>(pSourceNodeID, pDestNodeID, type);
		fogLinks[linkPtr->getLinkID()] = linkPtr;
		linkGraph->addLink(linkPtr->getSourceNodeID(), linkPtr->getDestNodeID(), linkPtr->getLinkID());
		linkGraph->addLink(linkPtr->getDestNodeID(), linkPtr->getSourceNodeID(), linkPtr->getLinkID());
	}
	void removeFogTopologyLink(FogTopologyLinkPtr linkPtr)
	{
		size_t search_id = linkPtr->getLinkID();
		linkGraph->removeLinkID(linkPtr->getSourceNodeID(), linkPtr->getDestNodeID());
		fogLinks.erase(linkPtr->getLinkID());
	}

	void printPlan()
	{
		linkGraph->print();
		cout << "Nodes IDs=";
		for (auto const& it : fogNodes)
		{
			cout << it.second->getNodeId() << ",";
		}

		cout << endl;

		cout << "Sensors IDs=";
		for (auto const& it : fogSensors)
		{
			cout << it.second->getSensorId() << ",";
		}
		cout << endl;

		cout << "slices:" << endl;
		linkGraph->removeRowAndCol();
	}

private:

	std::map<size_t,FogToplogyNodePtr> fogNodes;
	std::map<size_t,FogToplogySensorPtr> fogSensors;
	std::map<size_t,FogTopologyLinkPtr> fogLinks;
	size_t currentId;
	Graph* linkGraph;
};




#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
