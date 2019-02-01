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

#include "Topology/FogTopologyLink.hpp"
#include "Topology/FogTopologySensor.hpp"
#include "FogTopologyWorkerNode.hpp"
#include "Topology/FogTopologyEntry.hpp"


#include <iostream>                  // for std::cout
#include <utility>                   // for std::pair
#include <algorithm>                 // for std::for_each
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/dijkstra_shortest_paths.hpp>
#include <boost/graph/graphviz.hpp>
#include <boost/graph/graph_utility.hpp>


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
using namespace boost;

struct Vertex {size_t id; FogTopologyEntryPtr ptr;};
struct Edge { size_t id; FogTopologyLinkPtr ptr;};

using graph_t  = adjacency_list<listS, vecS, undirectedS, Vertex, Edge >;
using vertex_t = graph_traits<graph_t>::vertex_descriptor;
using edge_t   = graph_traits<graph_t>::edge_descriptor;


class FogGraph {
public:

	FogGraph(){};
	void addVertex(size_t id, FogTopologyEntryPtr ptr)
	{
	    boost::add_vertex(Vertex{id, ptr}, graph);
	}
	bool removeVertex(size_t search_id)
	{
		boost::graph_traits<graph_t>::vertex_iterator vi, vi_end, next;
		boost::tie(vi, vi_end) = vertices(graph);
		for (next = vi; vi != vi_end; vi = next)
		{
		  ++next;
		  if (graph[*vi].id == search_id)
		  {
			  remove_vertex(*vi, graph);
			  return true;
		  }
		}
		return false;
	}

	boost::graph_traits<graph_t>::vertex_descriptor getVertex(size_t search_id)
	{
		boost::graph_traits<graph_t>::vertex_iterator vi, vi_end, next;
		boost::tie(vi, vi_end) = vertices(graph);
		for (next = vi; vi != vi_end; vi = next)
		{
		  ++next;
		  if (graph[*vi].id == search_id)
		  {
			  return *vi;
		  }
		}
		return 0;
	}

	void addEdge(FogTopologyLinkPtr ptr, size_t sourceID, size_t destID)
	{
		size_t id = ptr->getID();
		boost::graph_traits<graph_t>::vertex_descriptor src = getVertex(sourceID);
		boost::graph_traits<graph_t>::vertex_descriptor dst = getVertex(destID);

	    boost::add_edge(src, dst, Edge{id, ptr}, graph);
	}

	bool removeEdge(size_t search_id)
	{
		boost::graph_traits<graph_t>::edge_iterator vi, vi_end, next;
		boost::tie(vi, vi_end) = edges(graph);
		for (next = vi; vi != vi_end; vi = next)
		{
		  ++next;
		  if (graph[*vi].id == search_id)
		  {
			  remove_edge(*vi, graph);
			  return true;
		  }
		}
		return false;
	}

	boost::graph_traits<graph_t>::edge_descriptor getEdge(size_t search_id)
	{
		boost::graph_traits<graph_t>::edge_iterator vi, vi_end, next;

		boost::tie(vi, vi_end) = edges(graph);
		for (next = vi; vi != vi_end; vi = next)
		{
		  ++next;
		  if (graph[*vi].id == search_id)
		  {
			  return *vi;
		  }
		}
//		return 0;
	}

	void print()
	{
		boost::write_graphviz(std::cout, graph, [&] (auto& out, auto v) {
		       out << "[label=\"" << graph[v].id << " type=" << graph[v].ptr->getEntryTypeString() << "\"]";
		      },
		      [&] (auto& out, auto e) {
		       out << "[label=\"" << graph[e].id << "\"]";
		    });
		    std::cout << std::flush;
	}


private:
    graph_t graph;

};


class FogTopologyPlan {

public:
  FogTopologyPlan()
	{
	  fGraph = new FogGraph();
	  currentId = 0;
	}

  FogTopologyWorkerNodePtr createFogWorkerNode()
  {
	  // TODO: check if id exists
	  FogTopologyWorkerNodePtr ptr = std::make_shared<FogTopologyWorkerNode>();
	  fGraph->addVertex(currentId, ptr);
	  ptr->setNodeId(currentId);
	  currentId++;
	  return ptr;
  }
  bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr)
  {
	  size_t search_id = ptr->getID();
	  return fGraph->removeVertex(search_id);
  }

  FogTopologySensorPtr createFogSensorNode() {
	  // TODO: check if id exists
	  FogTopologySensorPtr ptr = std::make_shared<FogTopologySensor>();
	  fGraph->addVertex(currentId, ptr);
	  ptr->setSensorId(currentId);
	  currentId++;
	  return ptr;
  }

  bool removeFogSensorNode(FogTopologySensorPtr ptr)
  {
	  size_t search_id = ptr->getID();
	  return fGraph->removeVertex(search_id);
  }

  FogTopologyLinkPtr createFogNodeLink(size_t pSourceNodeId, size_t pDestNodeId)
  {
    FogTopologyLinkPtr linkPtr = std::make_shared<FogTopologyLink>(pSourceNodeId, pDestNodeId);
    fGraph->addEdge(linkPtr, pSourceNodeId, pDestNodeId);
    return linkPtr;
  }

  bool removeFogTopologyLink(FogTopologyLinkPtr linkPtr)
  {
	  return fGraph->removeEdge(linkPtr->getID());
  }

  void printPlan() { fGraph->print();}

private:
  size_t currentId;
  FogGraph* fGraph;

};

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
