#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYPLAN_HPP_

#include <memory>
#include <string>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graph_traits.hpp>

#include "NESTopologyCoordinatorNode.hpp"
#include "NESTopologyEntry.hpp"
#include "NESTopologyLink.hpp"
#include "NESTopologySensorNode.hpp"
#include "NESTopologyWorkerNode.hpp"
#include "Util/CPUCapacity.hpp"

namespace iotdb {

#define MAX_NUMBER_OF_NODES 10 // TODO: make this dynamic

struct NESVertex {
  size_t id;
  NESTopologyEntryPtr ptr;
};
struct NESEdge {
  size_t id;
  NESTopologyLinkPtr ptr;
};

class NESGraph {

  using fogGraph_t = boost::adjacency_list<boost::listS, boost::vecS, boost::undirectedS, NESVertex, NESEdge>;
  using fogVertex_t = boost::graph_traits<fogGraph_t>::vertex_descriptor;
  using fogVertex_iterator = boost::graph_traits<fogGraph_t>::vertex_iterator;
  using fogEdge_t = boost::graph_traits<fogGraph_t>::edge_descriptor;
  using fogEdge_iterator = boost::graph_traits<fogGraph_t>::edge_iterator;

 public:
  NESGraph() {};

  const fogVertex_t getVertex(size_t search_id) const;
  bool hasVertex(size_t search_id) const;

  const std::vector<NESVertex> getAllVertex() const;
  bool addVertex(NESTopologyEntryPtr ptr);
  bool removeVertex(size_t search_id);

  NESTopologyEntryPtr getRoot() const;

  NESTopologyLinkPtr getLink(NESTopologyEntryPtr sourceNode, NESTopologyEntryPtr destNode) const;
  bool hasLink(NESTopologyEntryPtr sourceNode, NESTopologyEntryPtr destNode) const;
  bool hasLink(size_t searchId) const;

  const NESTopologyLinkPtr getEdge(size_t search_id) const;
  const std::vector<NESTopologyLinkPtr> getAllEdgesToNode(NESTopologyEntryPtr destNode) const;
  const std::vector<NESTopologyLinkPtr> getAllEdgesFromNode(NESTopologyEntryPtr srcNode) const;
  const std::vector<NESTopologyLinkPtr> getAllEdges() const;
  bool hasEdge(size_t search_id) const;

  bool addEdge(NESTopologyLinkPtr ptr);
  bool removeEdge(size_t search_id);

  std::string getGraphString() const;

 private:
  fogGraph_t graph;
};

typedef std::shared_ptr<NESGraph> NESGraphPtr;

class NESTopologyPlan {

 public:
  NESTopologyPlan();

  NESTopologyEntryPtr getRootNode() const;

  FogTopologyCoordinatorNodePtr createFogCoordinatorNode(const std::string ipAddr, CPUCapacity cpuCapacity);

  NESTopologyWorkerNodePtr createFogWorkerNode(const std::string ipAddr, CPUCapacity cpuCapacity);
  bool removeFogWorkerNode(NESTopologyWorkerNodePtr ptr);

  FogTopologySensorNodePtr createFogSensorNode(const std::string ipAddr, CPUCapacity cpuCapacity);
  bool removeFogSensorNode(FogTopologySensorNodePtr ptr);

  NESTopologyLinkPtr createFogTopologyLink(NESTopologyEntryPtr pSourceNode, NESTopologyEntryPtr pDestNode);
  bool removeFogTopologyLink(NESTopologyLinkPtr linkPtr);

  bool removeFogNode(NESTopologyEntryPtr ptr);

  std::string getTopologyPlanString() const;

  NESGraphPtr getFogGraph() const;

 private:

  size_t getNextFreeNodeId();
  size_t getNextFreeLinkId();
  size_t currentNodeId;
  size_t currentLinkId;
  NESGraphPtr fGraphPtr;
};
typedef std::shared_ptr<NESTopologyPlan> NESTopologyPlanPtr;
} // namespace iotdb

#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYPLAN_HPP_ */
