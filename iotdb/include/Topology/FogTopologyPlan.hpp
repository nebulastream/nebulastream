#ifndef INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_
#define INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_

#include <memory>
#include <string>

#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graph_traits.hpp>

#include "Topology/FogTopologyEntry.hpp"
#include "Topology/FogTopologyLink.hpp"
#include "Topology/FogTopologySensorNode.hpp"
#include "Topology/FogTopologyWorkerNode.hpp"

namespace iotdb {

#define MAX_NUMBER_OF_NODES 10 // TODO: make this dynamic

struct Vertex {
    size_t id;
    FogTopologyEntryPtr ptr;
};
struct Edge {
    size_t id;
    FogTopologyLinkPtr ptr;
};

using graph_t = boost::adjacency_list<boost::listS, boost::vecS, boost::undirectedS, Vertex, Edge>;
using vertex_t = boost::graph_traits<graph_t>::vertex_descriptor;
using vertex_iterator = boost::graph_traits<graph_t>::vertex_iterator;
using edge_t = boost::graph_traits<graph_t>::edge_descriptor;
using edge_iterator = boost::graph_traits<graph_t>::edge_iterator;

class FogGraph {
  public:
    FogGraph(){};

    const vertex_t getVertex(size_t search_id) const;
    bool hasVertex(size_t search_id) const;

    bool addVertex(FogTopologyEntryPtr ptr);
    bool removeVertex(size_t search_id);

    FogTopologyEntryPtr getRoot();

    FogTopologyLinkPtr getLink(FogTopologyEntryPtr sourceNode, FogTopologyEntryPtr destNode) const;
    bool hasLink(FogTopologyEntryPtr sourceNode, FogTopologyEntryPtr destNode) const;

    const Edge* getEdge(size_t search_id) const;
    bool hasEdge(size_t search_id) const;

    bool addEdge(FogTopologyLinkPtr ptr);
    bool removeEdge(size_t search_id);

    std::string getGraphString();

  private:
    graph_t graph;
};

class FogTopologyPlan {

  public:
    FogTopologyPlan();

    FogTopologyEntryPtr getRootNode() const;

    FogTopologyWorkerNodePtr createFogWorkerNode();
    bool removeFogWorkerNode(FogTopologyWorkerNodePtr ptr);

    FogTopologySensorNodePtr createFogSensorNode();
    bool removeFogSensorNode(FogTopologySensorNodePtr ptr);

    FogTopologyLinkPtr createFogTopologyLink(FogTopologyEntryPtr pSourceNode, FogTopologyEntryPtr pDestNode);
    bool removeFogTopologyLink(FogTopologyLinkPtr linkPtr);

    std::string getTopologyPlanString() const;

  private:
    size_t getNextFreeNodeId();

    size_t currentId;
    FogGraph* fGraph;
};
} // namespace iotdb

#endif /* INCLUDE_TOPOLOGY_FOGTOPOLOGYPLAN_HPP_ */
