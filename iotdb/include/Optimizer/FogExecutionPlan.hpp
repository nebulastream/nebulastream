#ifndef IOTDB_FOGEXECUTIONPLAN_HPP
#define IOTDB_FOGEXECUTIONPLAN_HPP

#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/ExecutionNodeLink.hpp>
#include <boost/graph/adjacency_list.hpp>
#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include <cpprest/json.h>

namespace iotdb {

using namespace web;

struct ExecutionVertex {
  int id;
  ExecutionNodePtr ptr;
};
struct ExecutionEdge {
  int id;
  ExecutionNodeLinkPtr ptr;
};
using executionGraph_t = boost::adjacency_list<boost::listS,
                                               boost::vecS,
                                               boost::undirectedS,
                                               ExecutionVertex,
                                               ExecutionEdge>;
using executionVertex_t = boost::graph_traits<executionGraph_t>::vertex_descriptor;
using executionVertex_iterator = boost::graph_traits<executionGraph_t>::vertex_iterator;
using executionEdge_t = boost::graph_traits<executionGraph_t>::edge_descriptor;
using executionEdge_iterator = boost::graph_traits<executionGraph_t>::edge_iterator;

class ExecutionGraph {

 private:
  executionGraph_t graph;

 public:
  ExecutionGraph() {};

  ExecutionNodePtr getRoot();

  const executionVertex_t getVertex(int search_id) const;

  bool hasVertex(int search_id) const;

  bool addVertex(ExecutionNodePtr ptr);

  vector<ExecutionVertex> getAllVertex() const;

  bool removeVertex(int search_id);

  const ExecutionNodePtr getNode(int search_id) const;

  ExecutionNodeLinkPtr getLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const;

  bool hasLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const;

  const ExecutionEdge *getEdge(int search_id) const;

  vector<ExecutionEdge> getAllEdges() const;

  bool hasEdge(int search_id) const;

  bool addEdge(ExecutionNodeLinkPtr ptr);

  std::string getGraphString();

  /**
   * @brief get all edges coming to the node
   * @param destNode
   * @return
   */
  const vector<ExecutionEdge> getAllEdgesToNode(ExecutionNodePtr destNode) const;

  /**
   * @brief get all edges starting from the node.
   * @param srcNode
   * @return vector of edges
   */
  const vector<ExecutionEdge> getAllEdgesFromNode(ExecutionNodePtr srcNode) const;
};

class FogExecutionPlan {

 public:
  FogExecutionPlan();

  ExecutionNodePtr getRootNode() const;

  ExecutionNodePtr
  createExecutionNode(std::string operatorName, std::string nodeName, FogTopologyEntryPtr fogNode,
                      OperatorPtr executableOperator);

  ExecutionNodeLinkPtr createExecutionNodeLink(ExecutionNodePtr src, ExecutionNodePtr dst);

  std::string getTopologyPlanString() const;

  std::shared_ptr<ExecutionGraph> getExecutionGraph() const;

  bool hasVertex(int search_id);

  ExecutionNodePtr getExecutionNode(int search_id);

  json::value getExecutionGraphAsJson() const;

 private:
  std::shared_ptr<ExecutionGraph> exeGraphPtr;

  vector<json::value> getChildrenNode(ExecutionNodePtr fogParentNode) const;
};
};

#endif //IOTDB_FOGEXECUTIONPLAN_HPP
