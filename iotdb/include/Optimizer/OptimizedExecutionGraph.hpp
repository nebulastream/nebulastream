#ifndef IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP
#define IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP

#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/ExecutionNodeLink.hpp>
#include <boost/graph/adjacency_list.hpp>

namespace iotdb {

    struct ExecutionVertex {
        int id;
        ExecutionNodePtr ptr;
    };
    struct ExecutionEdge {
        int id;
        ExecutionNodeLinkPtr ptr;
    };

    using graph_t = boost::adjacency_list<boost::listS, boost::vecS, boost::undirectedS, ExecutionVertex, ExecutionEdge>;
    using vertex_t = boost::graph_traits<graph_t>::vertex_descriptor;
    using vertex_iterator = boost::graph_traits<graph_t>::vertex_iterator;
    using edge_t = boost::graph_traits<graph_t>::edge_descriptor;
    using edge_iterator = boost::graph_traits<graph_t>::edge_iterator;

    class ExecutionGraph {

    public:
        ExecutionGraph() {}

        ExecutionVertex getRoot();

        const vertex_t getVertex(int search_id) const;

        bool hasVertex(int search_id) const;

        bool addVertex(ExecutionNodePtr ptr);

        bool removeVertex(int search_id);

        ExecutionNodeLinkPtr getLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const;

        bool hasLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const;

        const ExecutionEdge *getEdge(int search_id) const;

        bool hasEdge(int search_id) const;

        bool addEdge(ExecutionNodeLinkPtr ptr);

        std::string getGraphString();

    private:
        graph_t graph;

    };

    class OptimizedExecutionGraph {

    public:
        OptimizedExecutionGraph();

        ExecutionNodePtr getRootNode() const;

        ExecutionNodePtr
        createExecutionNode(std::string operatorName, std::string nodeName, FogTopologyEntryPtr fogNode,
                            OperatorPtr executableOperator);

        ExecutionNodeLinkPtr createExecutionNodeLink(ExecutionNodePtr src, ExecutionNodePtr dst);

        std::string getTopologyPlanString() const;

        ExecutionGraph getExecutionGraph();

    private:
        size_t currentId;
        ExecutionGraph *fGraph;
    };
};

#endif //IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP
