#ifndef INCLUDE_TOPOLOGY_NESTOPOLOGYGRAPH_HPP_
#define INCLUDE_TOPOLOGY_NESTOPOLOGYGRAPH_HPP_

#include <Topology/NESTopologyEntry.hpp>
#include <Topology/NESTopologyLink.hpp>
#undef U
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/graph_traits.hpp>
#include <boost/graph/graphviz.hpp>


namespace NES {

struct NESVertex {
    size_t id;
    NESTopologyEntryPtr ptr;
};
struct NESEdge {
    size_t id;
    NESTopologyLinkPtr ptr;
};

class NESTopologyGraph {

    /**
   * defining the classes used from the boot tree library
   */
    using nesGraph_t = boost::adjacency_list<boost::listS, boost::vecS, boost::undirectedS, NESVertex, NESEdge>;
    using nesVertex_t = boost::graph_traits<nesGraph_t>::vertex_descriptor;
    using nesVertex_iterator = boost::graph_traits<nesGraph_t>::vertex_iterator;
    using nesEdge_t = boost::graph_traits<nesGraph_t>::edge_descriptor;
    using nesEdge_iterator = boost::graph_traits<nesGraph_t>::edge_iterator;

  public:
    NESTopologyGraph() {
    }

    /**
   * @brief method to get a node/vertex by an id
   * @param id of the node
   * @return vertex_descriptor of the node
   */
    const nesVertex_t getVertex(size_t vertexId) const;

    /**
   * @brief method to check if a node/vertex exists
   * @param id of the node
   * @return bool indicating if node exists
   */
    bool hasVertex(size_t vertexId) const;

    /**
   * @brief method to get all nodes/vertices
   * @return vector containing nodes as NESVertex with an id and an NESTopologyEntryPtr
   */
    const std::vector<NESVertex> getAllVertex() const;

    /**
   * @brief method to add a node/vertex
   * @param NESTopologyEntryPtr to be inserted
   * @return bool indicating the success of the insert (if already exist, false is returned)
   */
    bool addVertex(NESTopologyEntryPtr ptr);

    /**
   * @brief method to remove a node/vertex
   * @param id of the node to be deleted
   * @bool indicating the success of the deletion (if node does not exists, false is returned)
   */
    bool removeVertex(size_t vertexId);

    /**
   * @brief method to get the root of the graph
   * @return NESTopologyEntryPtr to the root node
   */
    NESTopologyEntryPtr getRoot() const;

    /**
   * @brief method to get the link between two nodes/verticies
   * @param NESTopologyEntryPtr to node one
   * @param NESTopologyEntryPtr to node two
   * @return NESTopologyEntryPtr with the link, otherwise a nullptr
   */
    NESTopologyLinkPtr getLink(NESTopologyEntryPtr sourceNode,
                               NESTopologyEntryPtr destNode) const;

    /**
   * @brief method to test if a link between two nodes/verticies exists
   * @param NESTopologyEntryPtr to node one
   * @param NESTopologyEntryPtr to node two
   * @return NESTopologyEntryPtr with the link, otherwise a nullptr
   */
    bool hasLink(NESTopologyEntryPtr sourceNode,
                 NESTopologyEntryPtr destNode) const;

    /**
   * @brief method to check if a node has any connection within the graph
   * @param id of the to to test
   * @return bool indicating if the node is connected
   */
    bool hasLink(size_t edgeId) const;

    /**
   * @brief method to get the first edge
   * @param id of the node to test
   * @return NESTopologyEntryPtr with the link, otherwise a nullptr
   */
    const NESTopologyLinkPtr getEdge(size_t edgeId) const;

    /**
   * @brief test if a node has edges
   * @param id of the node to test
   * @return bool indicating if the node has an edge
   */
    bool hasEdge(size_t edgeId) const;

    /**
   * @brief method to add an edge
   * @param NESTopologyLinkPtr of the link to add
   * @return bool indicating the success of the insertion
   */
    bool addEdge(NESTopologyLinkPtr ptr);

    /**
   * @brief method remove an edge
   * @param id of the edge to be delete
   * @return bool indicating the success of the deletion
   */
    bool removeEdge(size_t edgeId);

    /**
   * @brief method to get all edges that lead to a given node/vertex
   * @param NESTopologyEntryPtr to destination node
   * @return vector with NESTopologyLinkPtr
   */
    const std::vector<NESTopologyLinkPtr> getAllEdgesToNode(
        NESTopologyEntryPtr destNode) const;

    /**
   * @brief method to get all edges that start from to a given node/vertex
   * @param NESTopologyEntryPtr to source node
   * @return vector with NESTopologyLinkPtr
   */
    const std::vector<NESTopologyLinkPtr> getAllEdgesFromNode(
        NESTopologyEntryPtr srcNode) const;

    /**
   * @brief method to get all all edges in the graph
   * @return vector with NESTopologyLinkPtr
   *
   */
    const std::vector<NESTopologyLinkPtr> getAllEdges() const;

    /**
   * @brief method to get the graph as a string
   * @return string containing the graph
   */
    std::string getGraphString() const;

    /**
  * @brief method to get the nodes in the graph as a string
  * @return string containing the graph
  */
    std::string getNodesString() const;

    /**
  * @brief method to get the edges in the graph as a string
  * @return string containing the graph
  */
    std::string getEdgesString() const;

    /**
   * @brief method to get all nodes with this ip
   * @param ip as string
   * @return vector containing the nodes as NESTopologyEntryPtr
   */
    const std::vector<NESTopologyEntryPtr> getVertexByIp(std::string ip) const;

    /**
   * @brief method to get all nodes with this id
   * @param id as size_t
   * @return vector containing the nodes as NESTopologyEntryPtr
   */
    const std::vector<NESTopologyEntryPtr> getVertexById(size_t id) const;

  private:
    nesGraph_t graph;
};

typedef std::shared_ptr<NESTopologyGraph> NESTopologyGraphPtr;

}// namespace NES
#endif /* INCLUDE_TOPOLOGY_NESTOPOLOGYGRAPH_HPP_ */
