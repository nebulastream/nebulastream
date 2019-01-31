#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_

#include <algorithm>
#include <bits/stdc++.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <boost/numeric/ublas/io.hpp>
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/numeric/ublas/matrix_proxy.hpp>

#include "Topology/FogTopologyLink.hpp"
#include "Topology/FogTopologyNode.hpp"
#include "Topology/FogTopologySensor.hpp"

#include <iostream>                  // for std::cout
#include <utility>                   // for std::pair
#include <algorithm>                 // for std::for_each
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/dijkstra_shortest_paths.hpp>
#include <boost/graph/graphviz.hpp>


#define MAX_NUMBER_OF_NODES 10 // TODO: make this dynamic
/**
 * Links:
 * 		- https://www.boost.org/doc/libs/1_61_0/libs/graph/doc/file_dependency_example.html
 * 		- https://www.boost.org/doc/libs/1_69_0/libs/graph/doc/index.html
 * 		- https://www.boost.org/doc/libs/1_49_0/libs/graph/doc/quick_tour.html
 * 		- https://theboostcpplibraries.com/boost.graph-vertices-and-edges
 * 		- https://stackoverflow.com/questions/3100146/adding-custom-vertices-to-a-boost-graph
 * TODOs:
 * 		- move functions to impl
 * 	Assumptions
 * 		-
 */

using namespace std;
using namespace boost::numeric::ublas;
using namespace boost;

struct Vertex {size_t id;};
struct Edge { size_t id; };

using graph_t  = adjacency_list<listS, vecS, undirectedS, Vertex, Edge >;
using vertex_t = graph_traits<graph_t>::vertex_descriptor;
using edge_t   = graph_traits<graph_t>::edge_descriptor;



class FogGraph {
public:
	void addVertex(size_t id)
	{
	    vertex_t u = boost::add_vertex(Vertex{id}, g);
	}
	void addEdge(size_t id, vertex_t u, vertex_t v)
	{
	    boost::add_edge(u, v, Edge{id}, g);
	}

	void print()
	{
		boost::write_graphviz(std::cout, g, [&] (auto& out, auto v) {
		       out << "[label=\"" << g[v].id << "\"]";
		      },
		      [&] (auto& out, auto e) {
		       out << "[label=\"" << g[e].id << "\"]";
		    });
		    std::cout << std::flush;
	}

private:
    graph_t g;

};



//class Graph {
//public:
//  Graph(int pNumberOfElements) {
//    numberOfRows = numberOfColums = 1;
//    mtx.resize(numberOfRows, numberOfColums);
//    mtx(0, 0) = 0;
//  }
//
//  void addNode() {
//    numberOfRows++;
//    numberOfColums++;
//    mtx.resize(numberOfRows, numberOfColums);
//  }
//  // function to add an edge to graph
//  void addLink(size_t nodeID1, size_t nodeID2, size_t linkID) {
//    // TODO: what to do if already exist?
//    mtx(nodeID1, nodeID2) = linkID;
//  }
//
//  size_t getLinkID(size_t nodeID1, size_t nodeID2) { return mtx(nodeID1, nodeID2); }
//
//  void removeLinkID(size_t nodeID1, size_t nodeID2) { mtx(nodeID1, nodeID2) = 0; }
//
//  std::string to_string(matrix<size_t> pMtx) {
//
//    std::stringstream ss;
//
//    // first line
//    ss << " | ";
//    for (size_t v = 0; v < pMtx.size1(); v++) {
//      if (v == 0)
//        ss << "-"
//           << " ";
//      else
//        ss << v << " ";
//    }
//
//    ss << endl;
//    for (size_t v = 0; v < pMtx.size2() * 2 + 3; v++)
//      ss << "-";
//    ss << endl;
//
//    for (size_t u = 0; u < pMtx.size1(); u++) {
//      if (u == 0)
//        ss << "-| ";
//      else
//        ss << u << "| ";
//      for (size_t v = 0; v < pMtx.size2(); v++) {
//        if (pMtx(u, v) == 0)
//          ss << "- ";
//        else
//          ss << pMtx(u, v) << " ";
//      }
//      ss << std::endl;
//    }
//    return ss.str();
//  }
//
//  std::string to_string() { return to_string(mtx); }
//
//  void print(matrix<size_t> pMtx) { std::cout << to_string(pMtx); }
//
//  void print() { print(mtx); }
//
//
//
//private:
//  size_t numberOfRows;
//  size_t numberOfColums;
//  matrix<size_t> mtx;
//  // create a typedef for the Graph type
//
//};

class FogTopologyPlan {

public:
  FogTopologyPlan() {
//    linkGraph = new Graph(MAX_NUMBER_OF_NODES);
    currentId = 1;
  }

  void addFogNode(FogTopologyNodePtr ptr) {
    //		if ( fogNodes.find("f") == m.end() ) {
    //		  // not found
    //		} else {
    //		  // found
    //		}
    // TODO: check if id exists
    fogNodes[currentId] = ptr;
    ptr->setNodeId(currentId);
    currentId++;
//    linkGraph->addNode();
  }
  void removeFogNode(FogTopologyNodePtr ptr) {
    size_t search_id = ptr->getNodeId();
    fogNodes.erase(search_id);
  }
  void listFogNodes() {
    cout << "fogNodes:";
    for (auto const &it : fogNodes) {
      cout << it.second->getNodeId() << ",";
    }
    cout << endl;
  }

  void addFogSensor(FogTopologySensorPtr ptr) {
    fogSensors[currentId] = ptr;
    ptr->setSensorId(currentId);
    currentId++;
//    linkGraph->addNode();
  }
  void removeFogSensor(FogTopologyNodePtr ptr) {
    size_t search_id = ptr->getNodeId();

    fogSensors.erase(search_id);
  }
  void listFogSensors() {
    cout << "fogSensors:";
    for (auto const &it : fogSensors) {
      cout << it.second->getSensorId() << ",";
    }
    cout << endl;
  }

  /**
   * Support half-duplex links?
   */
  void addFogTopologyLink(size_t pSourceNodeId, size_t pDestNodeId, LinkType type) {
    FogTopologyLinkPtr linkPtr = std::make_shared<FogTopologyLink>(pSourceNodeId, pDestNodeId, type);
    fogLinks[linkPtr->getLinkID()] = linkPtr;
//    linkGraph->addLink(linkPtr->getSourceNodeId(), linkPtr->getDestNodeId(), linkPtr->getLinkID());
//    linkGraph->addLink(linkPtr->getDestNodeId(), linkPtr->getSourceNodeId(), linkPtr->getLinkID());
  }
  void removeFogTopologyLink(FogTopologyLinkPtr linkPtr) {
    size_t search_id = linkPtr->getLinkID();
//    linkGraph->removeLinkID(linkPtr->getSourceNodeId(), linkPtr->getDestNodeId());
    fogLinks.erase(linkPtr->getLinkID());
  }

  std::string to_string() {
    std::stringstream ss;

//    ss << linkGraph->to_string();
    ss << "Nodes IDs=";
    for (auto const &it : fogNodes) {
      ss << it.second->getNodeId() << ",";
    }
    ss << endl;

    ss << "Sensors IDs=";
    for (auto const &it : fogSensors) {
      ss << it.second->getSensorId() << ",";
    }
    ss << endl;

    return ss.str();
  }

  void printPlan() { std::cout << to_string(); }

private:
  std::map<size_t, FogTopologyNodePtr> fogNodes;
  std::map<size_t, FogTopologySensorPtr> fogSensors;
  std::map<size_t, FogTopologyLinkPtr> fogLinks;
  size_t currentId;
//  Graph *linkGraph;
};

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
