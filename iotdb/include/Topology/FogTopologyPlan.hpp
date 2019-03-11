#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_

#include <algorithm>
#include <bits/stdc++.h>
#include <cassert>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
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

namespace iotdb {
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
using vertex_iterator = graph_traits<graph_t>::vertex_iterator;
using edge_t = graph_traits<graph_t>::edge_descriptor;
using edge_iterator = boost::graph_traits<graph_t>::edge_iterator;

class FogGraph {
public:
  FogGraph(){};

  bool hasVertex(size_t search_id) const {

    // build vertice iterator
    vertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
      ++next_vertex;

      // check for matching vertex
      if (graph[*vertex].id == search_id) {
        return true;
      }
    }

    return false;
  }

  const vertex_t getVertex(size_t search_id) const {

    assert(hasVertex(search_id));

    // build vertice iterator
    vertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
      ++next_vertex;

      // check for matching vertex
      if (graph[*vertex].id == search_id) {
        return *vertex;
      }
    }

    // should never happen
    return graph_traits<graph_t>::vertex_descriptor();
  }

  bool addVertex(FogTopologyEntryPtr ptr) {
    // does graph already contain vertex?
    if (hasVertex(ptr->getId())) {
      return false;
    }

    // add vertex
    boost::add_vertex(Vertex{ptr->getId(), ptr}, graph);
    return true;
  }

  bool removeVertex(size_t search_id) {

    // does graph contain vertex?
    if (hasVertex(search_id)) {
      remove_vertex(getVertex(search_id), graph);
      return true;
    }

    return false;
  }

  FogTopologyEntryPtr getRoot() {
    vertex_iterator vi, vi_end, next;
    boost::tie(vi, vi_end) = vertices(graph);
    for (next = vi; vi != vi_end; vi = next) {
      ++next;
      // first worker node is root TODO:change this
      if (graph[*vi].ptr->getEntryType() == Worker) {
        return graph[*vi].ptr;
      }
    }
    return 0;
  }

  FogTopologyLinkPtr getLink(FogTopologyEntryPtr sourceNode, FogTopologyEntryPtr destNode) const {

    // build edge iterator
    boost::graph_traits<graph_t>::edge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    // iterate over edges and check for matching links
    for (next_edge = edge; edge != edge_end; edge = next_edge) {
      ++next_edge;

      // check, if link does match
      auto current_link = graph[*edge].ptr;
      if (current_link->getSourceNodeId() == sourceNode->getId() &&
          current_link->getDestNodeId() == destNode->getId()) {
        return current_link;
      }
    }

    // no edge matched
    return nullptr;
  }

  bool hasLink(FogTopologyEntryPtr sourceNode, FogTopologyEntryPtr destNode) const {

    // edge found
    if (getLink(sourceNode, destNode) != nullptr) {
      return true;
    }

    // no edge found
    return false;
  }

  const Edge *getEdge(size_t search_id) const {

    // build edge iterator
    edge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    // iterate over edges
    for (next_edge = edge; edge != edge_end; edge = next_edge) {
      ++next_edge;

      // return matching edge
      if (graph[*edge].id == search_id) {
        return &graph[*edge];
      }
    }

    // no matching edge found
    return nullptr;
  }

  bool hasEdge(size_t search_id) const {

    // edge found
    if (getEdge(search_id) != nullptr) {
      return true;
    }

    // no edge found
    return false;
  }

  bool addEdge(FogTopologyLinkPtr ptr) {

    // link id is already in graph
    if (hasEdge(ptr->getId())) {
      return false;
    }

    // there is already a link between those two vertices
    if (hasLink(ptr->getSourceNode(), ptr->getDestNode())) {
      return false;
    }

    // check if vertices are in graph
    if (!hasVertex(ptr->getSourceNodeId()) || !hasVertex(ptr->getDestNodeId())) {
      return false;
    }

    // add edge with link
    auto src = getVertex(ptr->getSourceNodeId());
    auto dst = getVertex(ptr->getDestNodeId());
    boost::add_edge(src, dst, Edge{ptr->getId(), ptr}, graph);

    return true;
  }

  bool removeEdge(size_t search_id) {

    // does graph contain edge?
    if (!hasEdge(search_id)) {
      return false;
    }

    // check if vertices are in graph
    auto edge = getEdge(search_id);
    if (!hasVertex(edge->ptr->getSourceNodeId()) || !hasVertex(edge->ptr->getDestNodeId())) {
      return false;
    }

    // remove edge
    auto src = getVertex(edge->ptr->getSourceNodeId());
    auto dst = getVertex(edge->ptr->getDestNodeId());
    remove_edge(src, dst, graph);

    return true;
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

  FogTopologyEntryPtr getRootNode() { return fGraph->getRoot(); }

  FogTopologyWorkerNodePtr createFogWorkerNode() {

    // create worker node
    auto ptr = std::make_shared<FogTopologyWorkerNode>();
    ptr->setId(getNextFreeNodeId());
    fGraph->addVertex(ptr);

    return ptr;
  }

  bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr) { return fGraph->removeVertex(ptr->getId()); }

  FogTopologySensorNodePtr createFogSensorNode() {

    // create sensor node
    FogTopologySensorNodePtr ptr = std::make_shared<FogTopologySensorNode>();
    ptr->setId(getNextFreeNodeId());
    fGraph->addVertex(ptr);

    return ptr;
  }

  bool removeFogSensorNode(FogTopologySensorNodePtr ptr) { return fGraph->removeVertex(ptr->getId()); }

  FogTopologyLinkPtr createFogTopologyLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode) {

    // check if link already exists
    if (fGraph->hasLink(pSourceNode, pDestNode)) {
      // return already existing link
      return fGraph->getLink(pSourceNode, pDestNode);
    }

    // create new link
    auto linkPtr = std::make_shared<FogTopologyLink>(pSourceNode, pDestNode);
    assert(fGraph->addEdge(linkPtr));
    return linkPtr;
  }

  bool removeFogTopologyLink(FogTopologyLinkPtr linkPtr) { return fGraph->removeEdge(linkPtr->getId()); }

  std::string getTopologyPlanString() { return fGraph->getGraphString(); }

private:
  size_t getNextFreeNodeId() {
    while (fGraph->hasVertex(currentId)) {
      currentId++;
    }
    return currentId;
  }

  size_t currentId;
  FogGraph *fGraph;
};
} // namespace iotdb
#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
