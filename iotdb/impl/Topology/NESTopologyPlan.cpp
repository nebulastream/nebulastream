
#include "../../include/Topology/NESTopologyPlan.hpp"

#include <cassert>
#include <memory>
#include <sstream>
#include <string>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/graphviz.hpp>

#include "../../include/Topology/NESTopologyCoordinatorNode.hpp"
#include "../../include/Topology/NESTopologyEntry.hpp"
#include "../../include/Topology/NESTopologyLink.hpp"
#include "../../include/Topology/NESTopologySensorNode.hpp"
#include "../../include/Topology/NESTopologyWorkerNode.hpp"
#include "Util/CPUCapacity.hpp"

/**
 * Links for Boost Graph:
 * 		- https://www.boost.org/doc/libs/1_61_0/libs/graph/doc/file_dependency_example.html
 * 		- https://www.boost.org/doc/libs/1_69_0/libs/graph/doc/index.html
 * 		- https://www.boost.org/doc/libs/1_49_0/libs/graph/doc/quick_tour.html
 * 		- https://theboostcpplibraries.com/boost.graph-vertices-and-edges
 * 		- https://stackoverflow.com/questions/3100146/adding-custom-vertices-to-a-boost-graph
 */

namespace iotdb {

/* FogGraph ------------------------------------------------------------ */
bool NESGraph::hasVertex(size_t search_id) const {

  // build vertice iterator
  nesVertex_iterator vertex, vertex_end, next_vertex;
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

const NESGraph::nesVertex_t NESGraph::getVertex(size_t search_id) const {

  assert(hasVertex(search_id));

  // build vertice iterator
  nesVertex_iterator vertex, vertex_end, next_vertex;
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
  return nesVertex_t();
}

const std::vector<NESVertex> NESGraph::getAllVertex() const {
  std::vector<NESVertex> result = {};

  nesVertex_iterator vertex, vertex_end, next_vertex;
  boost::tie(vertex, vertex_end) = vertices(graph);

  for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
    ++next_vertex;
    result.push_back(graph[*vertex]);
  }

  return result;
}

bool NESGraph::addVertex(NESTopologyEntryPtr ptr) {
  // does graph already contain vertex?
  if (hasVertex(ptr->getId())) {
    return false;
  }

  // add vertex
  boost::add_vertex(NESVertex{ptr->getId(), ptr}, graph);
  return true;
}

bool NESGraph::removeVertex(size_t search_id) {

  // does graph contain vertex?
  if (hasVertex(search_id)) {
    clear_vertex(getVertex(search_id), graph);
    remove_vertex(getVertex(search_id), graph);
    return true;
  }

  return false;
}

NESTopologyEntryPtr NESGraph::getRoot() const {
  nesVertex_iterator vi, vi_end, next;
  boost::tie(vi, vi_end) = vertices(graph);
  for (next = vi; vi != vi_end; vi = next) {
    ++next;
    // first worker node is root TODO:change this
    if (graph[*vi].ptr->getEntryType() == Coordinator) {
      return graph[*vi].ptr;
    }
  }
  return 0;
}

NESTopologyLinkPtr NESGraph::getLink(NESTopologyEntryPtr sourceNode, NESTopologyEntryPtr destNode) const {

  // build edge iterator
  nesEdge_iterator edge, edge_end, next_edge;
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

bool NESGraph::hasLink(const NESTopologyEntryPtr sourceNode, const NESTopologyEntryPtr destNode) const {

  return getLink(sourceNode, destNode) != nullptr;
}

bool NESGraph::hasLink(size_t searchId) const {
  // build edge iterator
  nesEdge_iterator edge, edge_end, next_edge;
  boost::tie(edge, edge_end) = edges(graph);

  // iterate over edges and check for matching links
  for (next_edge = edge; edge != edge_end; edge = next_edge) {
    ++next_edge;

    // check, if link does match
    if (graph[*edge].id == searchId) {
      return true;
    }
  }
  return false;
}

const NESTopologyLinkPtr NESGraph::getEdge(size_t search_id) const {

  // build edge iterator
  nesEdge_iterator edge, edge_end, next_edge;
  boost::tie(edge, edge_end) = edges(graph);

  // iterate over edges
  for (next_edge = edge; edge != edge_end; edge = next_edge) {
    ++next_edge;

    // return matching edge
    if (graph[*edge].id == search_id) {
      return graph[*edge].ptr;
    }
  }

  // no matching edge found
  return nullptr;
}

const std::vector<NESTopologyLinkPtr> NESGraph::getAllEdgesToNode(NESTopologyEntryPtr destNode) const {

  std::vector<NESTopologyLinkPtr> result = {};

  nesEdge_iterator edge, edge_end, next_edge;
  boost::tie(edge, edge_end) = edges(graph);

  for (next_edge = edge; edge != edge_end; edge = next_edge) {
    ++next_edge;

    auto &fogEdgePtr = graph[*edge].ptr;
    if (fogEdgePtr->getDestNode()->getId() == destNode.get()->getId()) {

      result.push_back(fogEdgePtr);
    }
  }

  return result;
}

const std::vector<NESTopologyLinkPtr> NESGraph::getAllEdgesFromNode(NESTopologyEntryPtr srcNode) const {

  std::vector<NESTopologyLinkPtr> result = {};

  nesEdge_iterator edge, edge_end, next_edge;
  boost::tie(edge, edge_end) = edges(graph);

  for (next_edge = edge; edge != edge_end; edge = next_edge) {
    ++next_edge;

    auto &fogEdgePtr = graph[*edge].ptr;
    if (fogEdgePtr->getSourceNode()->getId() == srcNode.get()->getId()) {
      result.push_back(fogEdgePtr);
    }
  }

  return result;
}

const std::vector<NESTopologyLinkPtr> NESGraph::getAllEdges() const {

  std::vector<NESTopologyLinkPtr> result = {};
  nesEdge_iterator edge, edge_end, next_edge;
  boost::tie(edge, edge_end) = edges(graph);

  for (next_edge = edge; edge != edge_end; edge = next_edge) {
    ++next_edge;
    result.push_back(graph[*edge].ptr);
  }
  return result;
}

bool NESGraph::hasEdge(size_t search_id) const {

  // edge found
  if (getEdge(search_id) != nullptr) {
    return true;
  }

  // no edge found
  return false;
}

bool NESGraph::addEdge(NESTopologyLinkPtr ptr) {

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
  boost::add_edge(src, dst, NESEdge{ptr->getId(), ptr}, graph);

  return true;
}

bool NESGraph::removeEdge(size_t search_id) {

  // does graph contain edge?
  if (!hasEdge(search_id)) {
    return false;
  }

  // check if vertices are in graph
  auto edgePtr = getEdge(search_id);
  if (!hasVertex(edgePtr->getSourceNodeId()) || !hasVertex(edgePtr->getDestNodeId())) {
    return false;
  }

  // remove edge
  auto src = getVertex(edgePtr->getSourceNodeId());
  auto dst = getVertex(edgePtr->getDestNodeId());
  remove_edge(src, dst, graph);

  return true;
}

std::string NESGraph::getGraphString() const {
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

/* FogTopologyPlan ----------------------------------------------------- */
NESTopologyPlan::NESTopologyPlan() {
  fGraphPtr = std::make_shared<NESGraph>();
  currentNodeId = 0;
  currentLinkId = 0;
}

NESTopologyEntryPtr NESTopologyPlan::getRootNode() const { return fGraphPtr->getRoot(); }

NESTopologyCoordinatorNodePtr NESTopologyPlan::createNESCoordinatorNode(std::string ipAddr, CPUCapacity cpuCapacity) {
  // create coordinator node
  size_t nodeId = getNextFreeNodeId();
  auto ptr = std::make_shared<NESTopologyCoordinatorNode>(nodeId, ipAddr);
  ptr->setCpuCapacity(cpuCapacity);
  fGraphPtr->addVertex(ptr);
  return ptr;
}

NESTopologyWorkerNodePtr NESTopologyPlan::createNESWorkerNode(std::string ipAddr, CPUCapacity cpuCapacity) {
  // create worker node
  size_t nodeId = getNextFreeNodeId();
  auto ptr = std::make_shared<NESTopologyWorkerNode>(nodeId, ipAddr);
  ptr->setCpuCapacity(cpuCapacity);
  fGraphPtr->addVertex(ptr);
  return ptr;
}

NESTopologySensorNodePtr NESTopologyPlan::createNESSensorNode(std::string ipAddr, CPUCapacity cpuCapacity) {
  // create sensor node
  size_t nodeId = getNextFreeNodeId();
  auto ptr = std::make_shared<NESTopologySensorNode>(nodeId, ipAddr);
  ptr->setCpuCapacity(cpuCapacity);
  fGraphPtr->addVertex(ptr);
  return ptr;
}

bool NESTopologyPlan::removeNESWorkerNode(NESTopologyWorkerNodePtr ptr) {
  return fGraphPtr->removeVertex(ptr->getId());
}

bool NESTopologyPlan::removeNESSensorNode(NESTopologySensorNodePtr ptr) {
  return fGraphPtr->removeVertex(ptr->getId());
}

bool NESTopologyPlan::removeNESNode(NESTopologyEntryPtr ptr) {
  return fGraphPtr->removeVertex(ptr->getId());
}

NESTopologyLinkPtr NESTopologyPlan::createNESTopologyLink(NESTopologyEntryPtr pSourceNode,
                                                          NESTopologyEntryPtr pDestNode) {

  // check if link already exists
  if (fGraphPtr->hasLink(pSourceNode, pDestNode)) {
    // return already existing link
    return fGraphPtr->getLink(pSourceNode, pDestNode);
  }

  // create new link
  size_t linkId = getNextFreeLinkId();
  auto linkPtr = std::make_shared<NESTopologyLink>(linkId, pSourceNode, pDestNode);
  assert(fGraphPtr->addEdge(linkPtr));
  return linkPtr;
}

bool NESTopologyPlan::removeNESTopologyLink(NESTopologyLinkPtr linkPtr) {
  return fGraphPtr->removeEdge(linkPtr->getId());
}

std::string NESTopologyPlan::getTopologyPlanString() const { return fGraphPtr->getGraphString(); }

size_t NESTopologyPlan::getNextFreeNodeId() {
  while (fGraphPtr->hasVertex(currentNodeId)) {
    currentNodeId++;
  }
  return currentNodeId;
}

size_t NESTopologyPlan::getNextFreeLinkId() {
  while (fGraphPtr->hasLink(currentLinkId)) {
    currentLinkId++;
  }
  return currentLinkId;
}

NESGraphPtr NESTopologyPlan::getNESGraph() const {
  return fGraphPtr;
}

} // namespace iotdb