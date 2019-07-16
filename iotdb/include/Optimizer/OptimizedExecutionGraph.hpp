#ifndef IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP
#define IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP

#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/ExecutionNodeLink.hpp>
#include <boost/graph/adjacency_list.hpp>

namespace iotdb {

    struct Vertex {
        size_t id;
        ExecutionNodePtr ptr;
    };
    struct Edge {
        size_t id;
        ExecutionNodeLinkPtr ptr;
    };

    using graph_t = boost::adjacency_list<boost::listS, boost::vecS, boost::undirectedS, Vertex, Edge>;
    using vertex_t = boost::graph_traits<graph_t>::vertex_descriptor;
    using vertex_iterator = boost::graph_traits<graph_t>::vertex_iterator;
    using edge_t = boost::graph_traits<graph_t>::edge_descriptor;
    using edge_iterator = boost::graph_traits<graph_t>::edge_iterator;

    class ExecutionGraph {

    public:
        ExecutionGraph() {}

        const vertex_t getVertex(size_t search_id) const;

        bool hasVertex(size_t search_id) const;

        const std::vector<Vertex> getAllVertex() const;

        bool addVertex(FogTopologyEntryPtr ptr);

        bool removeVertex(size_t search_id);

        Vertex getRoot();

        Vertex getLink(FogTopologyEntryPtr sourceNode, FogTopologyEntryPtr destNode) const;

        bool hasLink(FogTopologyEntryPtr sourceNode, FogTopologyEntryPtr destNode) const;

        const Edge *getEdge(size_t search_id) const;

        const std::vector<Edge> getAllEdgesToNode(FogTopologyEntryPtr destNode) const;

        bool hasEdge(size_t search_id) const;

        bool addEdge(ExecutionNodeLinkPtr ptr);

        bool removeEdge(size_t search_id);

        std::string getGraphString();

    private:
        graph_t graph;

    };

    class OptimizedExecutionGraph {

    public:
        OptimizedExecutionGraph();

        ExecutionNodePtr getRootNode() const;

        ExecutionNodePtr
        createExecutionNode(std::string operatorName, std::string nodeName, FogTopologyEntryPtr nodeAndOperator);

        bool removeExecutionNode(ExecutionNodePtr ptr);

        ExecutionNodeLinkPtr createExecutionNodeLink(FogTopologyEntryPtr src, FogTopologyEntryPtr dst);

        bool removeFogTopologyLink(ExecutionNodeLinkPtr linkPtr);

        std::string getTopologyPlanString() const;

        ExecutionGraph getExecutionGraph();

    private:
        size_t getNextFreeNodeId();

        size_t currentId;
        ExecutionGraph *fGraph;
    };
};

#endif //IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP
