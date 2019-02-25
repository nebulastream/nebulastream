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

#include "FogTopologyWorkerNode.hpp"
#include "Topology/FogTopologyEntry.hpp"
#include "Topology/FogTopologyLink.hpp"
#include "Topology/FogTopologySensorNode.hpp"

#include <algorithm> // for std::for_each
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/dijkstra_shortest_paths.hpp>
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/graph_utility.hpp>
#include <boost/graph/graphviz.hpp>
#include <boost/graph/topological_sort.hpp>

#include <iostream> // for std::cout
#include <utility>  // for std::pair

namespace iotdb{
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

struct Vertex {
  size_t id;
  FogTopologyEntryPtr ptr;
};
struct Edge {
  size_t id;
  FogTopologyLinkPtr ptr;
};

using graph_t = adjacency_list<listS, vecS, undirectedS, Vertex, Edge>;
using vertex_t = graph_traits<graph_t>::vertex_descriptor;
using edge_t = graph_traits<graph_t>::edge_descriptor;

class FogGraph {
public:
  FogGraph(){};
  void addVertex(FogTopologyEntryPtr ptr) { boost::add_vertex(Vertex{ptr->getId(), ptr}, graph); }
  bool removeVertex(size_t search_id) {
    boost::graph_traits<graph_t>::vertex_iterator vi, vi_end, next;
    boost::tie(vi, vi_end) = vertices(graph);
    for (next = vi; vi != vi_end; vi = next) {
      ++next;
      if (graph[*vi].id == search_id) {
        remove_vertex(*vi, graph);
        return true;
      }
    }
    return false;
  }

  //

  FogTopologyEntryPtr getRoot() {
	    boost::graph_traits<graph_t>::vertex_iterator vi, vi_end, next;
	    boost::tie(vi, vi_end) = vertices(graph);
	    for (next = vi; vi != vi_end; vi = next) {
	      ++next;
	      //first worker node is root TODO:change this
	      if (graph[*vi].ptr->getEntryType() == Worker)
	      {
	    	  return graph[*vi].ptr;
	      }
	    }
	    return 0;
  }

  boost::graph_traits<graph_t>::vertex_descriptor getVertex(size_t search_id) {
    boost::graph_traits<graph_t>::vertex_iterator vi, vi_end, next;
    boost::tie(vi, vi_end) = vertices(graph);
    for (next = vi; vi != vi_end; vi = next) {
      ++next;
      if (graph[*vi].id == search_id) {
        return *vi;
      }
    }
    return 0;
  }

  void addEdge(FogTopologyLinkPtr ptr) {
    size_t id = ptr->getId();
    boost::graph_traits<graph_t>::vertex_descriptor src = getVertex(ptr->getSourceNodeId());
    boost::graph_traits<graph_t>::vertex_descriptor dst = getVertex(ptr->getDestNodeId());

    boost::add_edge(src, dst, Edge{id, ptr}, graph);
  }

  bool removeEdge(size_t search_id) {
    boost::graph_traits<graph_t>::edge_iterator vi, vi_end, next;
    boost::tie(vi, vi_end) = edges(graph);
    for (next = vi; vi != vi_end; vi = next) {
      ++next;
      if (graph[*vi].id == search_id) {
        remove_edge(*vi, graph);
        return true;
      }
    }
    return false;
  }

  boost::graph_traits<graph_t>::edge_descriptor getEdge(size_t search_id) {
    boost::graph_traits<graph_t>::edge_iterator vi, vi_end, next;

    boost::tie(vi, vi_end) = edges(graph);
    for (next = vi; vi != vi_end; vi = next) {
      ++next;
      if (graph[*vi].id == search_id) {
        return *vi;
      }
    }
    //		return 0;
  }

  std::string getGraphString() {
    std::stringstream ss;
    boost::write_graphviz(ss, graph,
                          [&](auto &out, auto v) {
                            out << "[label=\"" << graph[v].id << " type=" << graph[v].ptr->getEntryTypeString()
                                << "\"]";
                          },
                          [&](auto &out, auto e) { out << "[label=\"" << graph[e].id << "\"]"; });
    ss << std::flush;
    return ss.str();
  }

private:
  graph_t graph;
};

class FogTopologyPlan {

public:
  FogTopologyPlan() {
    fGraph = new FogGraph();
    currentId = 0;
  }

  FogTopologyEntryPtr getRootNode()
  {
	  return fGraph->getRoot();
  }
  FogTopologyWorkerNodePtr createFogWorkerNode() {
    // TODO: check if id exists
    FogTopologyWorkerNodePtr ptr = std::make_shared<FogTopologyWorkerNode>();
    ptr->setId(currentId);
    fGraph->addVertex(ptr);
    currentId++;
    return ptr;
  }
  bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr) {
    size_t search_id = ptr->getId();
    return fGraph->removeVertex(search_id);
  }

  FogTopologySensorNodePtr createFogSensorNode() {
    // TODO: check if id exists
    FogTopologySensorNodePtr ptr = std::make_shared<FogTopologySensorNode>();
    ptr->setId(currentId);
    fGraph->addVertex(ptr);
    currentId++;
    return ptr;
  }

  bool removeFogSensorNode(FogTopologySensorNodePtr ptr) {
    size_t search_id = ptr->getId();
    return fGraph->removeVertex(search_id);
  }

  FogTopologyLinkPtr createFogNodeLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode) {
    FogTopologyLinkPtr linkPtr = std::make_shared<FogTopologyLink>(pSourceNode, pDestNode);
    fGraph->addEdge(linkPtr);
    return linkPtr;
  }

  bool removeFogTopologyLink(FogTopologyLinkPtr linkPtr) { return fGraph->removeEdge(linkPtr->getId()); }

  std::string getTopologyPlanString() { return fGraph->getGraphString(); }


private:
  size_t currentId;
  FogGraph *fGraph;
};
}
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
