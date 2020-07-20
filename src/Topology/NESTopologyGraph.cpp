#include <Topology/NESTopologyGraph.hpp>
#include <Util/Logger.hpp>
namespace NES {

/* NESGraph ------------------------------------------------------------ */
bool NESTopologyGraph::hasVertex(size_t vertexId) const {
    // build vertice iterator
    nesVertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
        ++next_vertex;

        // check for matching vertex
        if (graph[*vertex].id == vertexId) {
            return true;
        }
    }

    return false;
}

const std::vector<NESTopologyEntryPtr> NESTopologyGraph::getVertexByIp(
    std::string ip) const {
    // build vertice iterator
    nesVertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);
    std::vector<NESTopologyEntryPtr> vec;

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
        ++next_vertex;

        // check for matching vertex
        if (graph[*vertex].ptr->getIp() == ip) {
            vec.push_back(graph[*vertex].ptr);
        }
    }

    // should never happen
    return vec;
}

const std::vector<NESTopologyEntryPtr> NESTopologyGraph::getVertexById(
    size_t id) const {
    // build vertice iterator
    nesVertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);
    std::vector<NESTopologyEntryPtr> vec;

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
        ++next_vertex;

        // check for matching vertex
        if (graph[*vertex].ptr->getId() == id) {
            vec.push_back(graph[*vertex].ptr);
        }
    }

    // should never happen
    return vec;
}

const NESTopologyGraph::nesVertex_t NESTopologyGraph::getVertex(
    size_t vertexId) const {

    bool success = hasVertex(vertexId);
    if(!success)
    {
        throw Exception("runtime error in hasVertex(vertexId)");
    }

    // build vertice iterator
    nesVertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
        ++next_vertex;

        // check for matching vertex
        if (graph[*vertex].id == vertexId) {
            return *vertex;
        }
    }

    // should never happen
    return nesVertex_t();
}

const std::vector<NESVertex> NESTopologyGraph::getAllVertex() const {
    std::vector<NESVertex> result = {};

    nesVertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);

    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
        ++next_vertex;
        result.push_back(graph[*vertex]);
    }

    return result;
}

bool NESTopologyGraph::addVertex(NESTopologyEntryPtr ptr) {
    // does graph already contain vertex?
    if (hasVertex(ptr->getId())) {
        NES_DEBUG("NESTopologyGraph: addVertex error id already exists " << ptr->getId());
        return false;
    }

    // add vertex
    boost::add_vertex(NESVertex{ptr->getId(), ptr}, graph);
    return true;
}

bool NESTopologyGraph::removeVertex(size_t vertexId) {
    // does graph contain vertex?
    if (hasVertex(vertexId)) {
        clear_vertex(getVertex(vertexId), graph);
        remove_vertex(getVertex(vertexId), graph);
        return true;
    }

    return false;
}

NESTopologyEntryPtr NESTopologyGraph::getRoot() const {
    nesVertex_iterator vi, vi_end, next;
    boost::tie(vi, vi_end) = vertices(graph);
    for (next = vi; vi != vi_end; vi = next) {
        ++next;
        // first worker node is root TODO:change this
        if (graph[*vi].ptr->getEntryType() == Worker) {
            return graph[*vi].ptr;
        }
    }
    return nullptr;
}

NESTopologyLinkPtr NESTopologyGraph::getLink(
    NESTopologyEntryPtr sourceNode, NESTopologyEntryPtr destNode) const {

    // build edge iterator
    nesEdge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    // iterate over edges and check for matching links
    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;

        // check, if link does match
        auto current_link = graph[*edge].ptr;
        if (current_link->getSourceNodeId() == sourceNode->getId()
            && current_link->getDestNodeId() == destNode->getId()) {
            return current_link;
        }
    }

    // no edge matched
    return nullptr;
}

bool NESTopologyGraph::hasLink(const NESTopologyEntryPtr sourceNode,
                               const NESTopologyEntryPtr destNode) const {

    return getLink(sourceNode, destNode) != nullptr;
}

bool NESTopologyGraph::hasLink(size_t edgeId) const {
    // build edge iterator
    nesEdge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    // iterate over edges and check for matching links
    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;

        // check, if link does match
        if (graph[*edge].id == edgeId) {
            return true;
        }
    }
    return false;
}

const NESTopologyLinkPtr NESTopologyGraph::getEdge(size_t edgeId) const {

    // build edge iterator
    nesEdge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    // iterate over edges
    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;

        // return matching edge
        if (graph[*edge].id == edgeId) {
            return graph[*edge].ptr;
        }
    }

    // no matching edge found
    return nullptr;
}

const std::vector<NESTopologyLinkPtr> NESTopologyGraph::getAllEdgesToNode(
    NESTopologyEntryPtr destNode) const {

    std::vector<NESTopologyLinkPtr> result = {};

    nesEdge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;

        auto& edgePtr = graph[*edge].ptr;
        if (edgePtr->getDestNode()->getId() == destNode.get()->getId()) {

            result.push_back(edgePtr);
        }
    }

    return result;
}

const std::vector<NESTopologyLinkPtr> NESTopologyGraph::getAllEdgesFromNode(
    NESTopologyEntryPtr srcNode) const {

    std::vector<NESTopologyLinkPtr> result = {};

    nesEdge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;

        auto& edgePtr = graph[*edge].ptr;
        if (edgePtr->getSourceNode()->getId() == srcNode.get()->getId()) {
            result.push_back(edgePtr);
        }
    }

    return result;
}

const std::vector<NESTopologyLinkPtr> NESTopologyGraph::getAllEdges() const {

    std::vector<NESTopologyLinkPtr> result = {};
    nesEdge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;
        result.push_back(graph[*edge].ptr);
    }
    return result;
}

bool NESTopologyGraph::hasEdge(size_t edgeId) const {

    // edge found
    if (getEdge(edgeId) != nullptr) {
        return true;
    }
    // no edge found
    return false;
}

bool NESTopologyGraph::addEdge(NESTopologyLinkPtr ptr) {

    // link id is already in graph
    if (hasEdge(ptr->getId())) {
        NES_ERROR("NESTopologyGraph::addEdge: link id is already in graph=" << ptr->getId());
        return false;
    }

    // there is already a link between those two vertices
    if (hasLink(ptr->getSourceNode(), ptr->getDestNode())) {
        NES_ERROR("NESTopologyGraph::addEdge: there is already a link between nodes");
        return false;
    }

    // check if vertices are in graph
    if (!hasVertex(ptr->getSourceNodeId()) || !hasVertex(ptr->getDestNodeId())) {
        NES_ERROR("NESTopologyGraph::addEdge: one of the vertices is not in the graph");
        return false;
    }

    // add edge with link
    auto src = getVertex(ptr->getSourceNodeId());
    auto dst = getVertex(ptr->getDestNodeId());
    boost::add_edge(src, dst, NESEdge{ptr->getId(), ptr}, graph);

    return true;
}

bool NESTopologyGraph::removeEdge(size_t edgeId) {

    // does graph contain edge?
    if (!hasEdge(edgeId)) {
        return false;
    }

    // check if vertices are in graph
    auto edgePtr = getEdge(edgeId);
    if (!hasVertex(edgePtr->getSourceNodeId())
        || !hasVertex(edgePtr->getDestNodeId())) {
        return false;
    }

    // remove edge
    auto src = getVertex(edgePtr->getSourceNodeId());
    auto dst = getVertex(edgePtr->getDestNodeId());
    remove_edge(src, dst, graph);

    return true;
}

std::string NESTopologyGraph::getGraphString() const {
    std::stringstream ss;
    boost::write_graphviz(
        ss,
        graph,
        [&](auto& out, auto v) {
            out << "[label=\"" << graph[v].id << " type="
                << graph[v].ptr->getEntryTypeString() << "\"]";
        },
        [&](auto& out, auto e) {
            out << "[label=\"" << graph[e].id << "\"]";
        });
    ss << std::flush;
    return ss.str();
}

std::string NESTopologyGraph::getNodesString() const {
    std::stringstream ss;
    boost::write_graphviz(
        ss,
        graph,
        [&](auto& out, auto v) {
            out << "[label=\"" << graph[v].id << " type="
                << graph[v].ptr->getEntryTypeString() << "\"]";
        });
    ss << std::flush;
    return ss.str();
}

std::string NESTopologyGraph::getEdgesString() const {
    std::stringstream ss;
    boost::write_graphviz(
        ss,
        graph,
        [&](auto& out, auto e) {
            out << "[label=\"" << graph[e].id << "\"]";
        });
    ss << std::flush;
    return ss.str();
}
}// namespace NES
