#ifndef NESEXECUTIONPLAN_HPP
#define NESEXECUTIONPLAN_HPP

#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/ExecutionNodeLink.hpp>
#include <boost/graph/adjacency_list.hpp>
#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include <cpprest/json.h>

namespace NES {

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

    const ExecutionEdge* getEdge(int search_id) const;

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

typedef std::shared_ptr<ExecutionGraph> ExecutionGraphPtr;

class NESExecutionPlan {

  public:
    NESExecutionPlan();

    void freeResources();

    ExecutionNodePtr getRootNode() const;

    /**
     * @brief Create execution node containing information about the chain of operators to be executed
     * @param operatorName: Name of the operator to be executed (contains "=>" separated actual operator names with their ids)
     * @param nodeName : name of the node (usually a unique identifier)
     * @param nesNode : information about actual nes node where the operators will be executed
     * @param executableOperator : chain of operators to be executed
     * @return pointer to the execution node
     */
    ExecutionNodePtr createExecutionNode(std::string operatorName, std::string nodeName, NESTopologyEntryPtr nesNode,
                                         OperatorPtr executableOperator);

    /**
     * @brief Create execution node link between sourceNode and destinationNode nodes
     * @param id : link id
     * @param sourceNode : node which generate the data
     * @param destinationNode : node which consumes the data
     * @param linkCapacity : capacity of the link
     * @param linkLatency : latency of the link
     * @return return execution node link pointer
     */
    ExecutionNodeLinkPtr createExecutionNodeLink(size_t id,
                                                 ExecutionNodePtr sourceNode,
                                                 ExecutionNodePtr destinationNode,
                                                 size_t linkCapacity,
                                                 size_t linkLatency);

    std::string getTopologyPlanString() const;

    ExecutionGraphPtr getExecutionGraph() const;

    bool hasVertex(int search_id);

    ExecutionNodePtr getExecutionNode(int search_id);

    json::value getExecutionGraphAsJson() const;

    long getTotalComputeTimeInMillis() const;
    void setTotalComputeTimeInMillis(long totalComputeTime);

  private:
    ExecutionGraphPtr exeGraphPtr;
    long totalComputeTimeInMillis;
    vector<json::value> getChildrenNode(ExecutionNodePtr nesParentNode) const;
};

typedef std::shared_ptr<NESExecutionPlan> NESExecutionPlanPtr;

}

#endif //NESEXECUTIONPLAN_HPP
