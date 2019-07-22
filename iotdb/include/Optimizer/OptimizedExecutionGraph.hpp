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
    using executionGraph_t = boost::adjacency_list<boost::listS, boost::vecS, boost::undirectedS, ExecutionVertex, ExecutionEdge>;
    using executionVertex_t = boost::graph_traits<executionGraph_t>::vertex_descriptor;
    using executionVertex_iterator = boost::graph_traits<executionGraph_t>::vertex_iterator;
    using executionEdge_t = boost::graph_traits<executionGraph_t>::edge_descriptor;
    using executionEdge_iterator = boost::graph_traits<executionGraph_t>::edge_iterator;

    class ExecutionGraph {

    private:
        executionGraph_t graph;

    public:
        ExecutionGraph();

        ExecutionNodePtr getRoot();

        const executionVertex_t getVertex(int search_id) const;

        bool hasVertex(int search_id) const;

        bool addVertex(ExecutionNodePtr ptr);

        bool removeVertex(int search_id);

        const ExecutionNodePtr getNode(int search_id) const;

        ExecutionNodeLinkPtr getLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const;

        bool hasLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const;

        const ExecutionEdge *getEdge(int search_id) const;

        bool hasEdge(int search_id) const;

        bool addEdge(ExecutionNodeLinkPtr ptr);

        std::string getGraphString();

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

        bool hasVertex(int search_id);

        ExecutionNodePtr getExecutionNode(int search_id);

    private:
        ExecutionGraph *fGraph;
    };
};

#endif //IOTDB_OPTIMIZEDEXECUTIONGRAPH_HPP
