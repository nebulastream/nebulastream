#ifndef INCLUDE_OPTIMIZER_EXECUTIONGRAPH_HPP_
#define INCLUDE_OPTIMIZER_EXECUTIONGRAPH_HPP_
#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/ExecutionNodeLink.hpp>
#include <boost/graph/adjacency_list.hpp>

namespace NES {
struct ExecutionVertex {
    size_t id;
    ExecutionNodePtr ptr;
};
struct ExecutionEdge {
    size_t id;
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
  public:
    ExecutionGraph(){};

    /**
     * @brief method to get the root node of the graph (with id 0)
     * @return ptr to the root node if exists, otherwise nullptr
     */
    ExecutionNodePtr getRoot();

    /**
     * @brief method to get a vertex of the graph
     * @param id of the vertex
     * @return vertex_descriptor of vertex, if it does not exists then it a empty vertex_descriptor is returned
     */
    const executionVertex_t getVertex(size_t searchId) const;

    /**
     * @brief method to check if a vertex exists
     * @param id of the vertex
     * @bool indicating existence
     */
    bool hasVertex(size_t searchId) const;

    /**
     * @brief mehtod to add a vertex
     * @param ptr to the new vertex
     * @return bool indicating success
     */
    bool addVertex(ExecutionNodePtr ptr);

    /**
     * @brief method to get all verticies of the graph
     * @return vector containing ExecutionVertex which comine the id and the ptr to the vertex
     */
    std::vector<ExecutionVertex> getAllVertex() const;

    /**
     * @brief method to remove a vertex
     * @param id of the vertex
     * @return bool indicating success
     */
    bool removeVertex(size_t searchId);

    /**
     * @brief method to return an execution node from the graph
     * @param id of the node
     * @return ptr to the execution node
     */
    const ExecutionNodePtr getNode(size_t searchId) const;

    /**
     * @brief method to get the link between two nodes
     * @param ptr to source node
     * @param ptr to destination node
     * @return execution node link ptr if exists, otherwise nullptr
     */
    ExecutionNodeLinkPtr getLink(ExecutionNodePtr sourceNode,
                                 ExecutionNodePtr destNode) const;

    /**
     * @brief method to test if a link exists
     * @param ptr to source node
     * @param ptr to destination node
     * @return bool indicating existence
     */
    bool hasLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const;

    /**
     * @brief method to get a vector containing all edges
     * @return vector containing all edges in the graph
     */
    std::vector<ExecutionEdge> getAllEdges() const;

    /**
     * @brief method to check if an edge exists
     * @param id of the edge
     * @bool indicating existence
     */
    bool hasEdge(size_t searchId) const;

    /**
     * @brief method to add an edge
     * @param execution node link ptr
     * @bool indicating success
     */
    bool addEdge(ExecutionNodeLinkPtr ptr);

    /**
     * @brief method to get the graph as a string
     * @return string created using boost::write_graphviz
     */
    std::string getGraphString();

    /**
     * @brief get all edges coming to the node
     * @param destNode
     * @return
     */
    const std::vector<ExecutionEdge> getAllEdgesToNode(
        ExecutionNodePtr destNode) const;

    /**
     * @brief get all edges starting from the node.
     * @param srcNode
     * @return vector of edges
     */
    const std::vector<ExecutionEdge> getAllEdgesFromNode(
        ExecutionNodePtr srcNode) const;

  private:
    executionGraph_t graph;

    /**
     * @brief method to get a raw pointer to an execution edge
     * @param id of the edge
     * @return raw pointer to edge
     */
    const ExecutionEdge* getEdge(size_t searchId) const;
};

typedef std::shared_ptr<ExecutionGraph> ExecutionGraphPtr;

}// namespace NES
#endif /* INCLUDE_OPTIMIZER_EXECUTIONGRAPH_HPP_ */
