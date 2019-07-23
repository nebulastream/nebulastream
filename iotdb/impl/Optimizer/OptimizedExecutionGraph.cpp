
#include <Optimizer/OptimizedExecutionGraph.hpp>
#include <boost/graph/graphviz.hpp>
#include <cpprest/json.h>

using namespace iotdb;
using namespace web;

ExecutionNodePtr ExecutionGraph::getRoot() {
    executionVertex_iterator vi, vi_end, next;
    boost::tie(vi, vi_end) = vertices(graph);
    for (next = vi; vi != vi_end; vi = next) {
        ++next;
        if (graph[*vi].ptr->getId() == 0) {
            return graph[*vi].ptr;
        }
    }

    return 0;
};

bool ExecutionGraph::hasVertex(int search_id) const {
    // build vertex iterator
    executionVertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
        ++next_vertex;

        // check for matching vertex
        if (graph[*vertex].id == search_id) {
            return true;
        }
    }

    return false;
};

bool ExecutionGraph::addVertex(ExecutionNodePtr ptr) {
    // does graph already contain vertex?
    if (hasVertex(ptr->getId())) {
        return false;
    }

    // add vertex
    boost::add_vertex(ExecutionVertex{ptr->getId(), ptr}, graph);
    return true;
};

bool ExecutionGraph::removeVertex(int search_id) {
    // does graph contain vertex?
    if (hasVertex(search_id)) {
        remove_vertex(getVertex(search_id), graph);
        return true;
    }

    return false;
};

const executionVertex_t ExecutionGraph::getVertex(int search_id) const {
    assert(hasVertex(search_id));

    // build vertex iterator
    executionVertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
        ++next_vertex;

        // check for matching vertex
        if (graph[*vertex].id == search_id) {
            return *vertex;
        }
    }

    // should never happen
    return executionVertex_t();
};

const ExecutionNodePtr ExecutionGraph::getNode(int search_id) const {
    // build vertex iterator
    executionVertex_iterator vertex, vertex_end, next_vertex;
    boost::tie(vertex, vertex_end) = vertices(graph);

    // iterator over vertices
    for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
        ++next_vertex;

        // check for matching vertex
        if (graph[*vertex].id == search_id) {
            return graph[*vertex].ptr;
        }
    }

    // should never happen
    return nullptr;
}

ExecutionNodeLinkPtr ExecutionGraph::getLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const {
    // build edge iterator
    boost::graph_traits<executionGraph_t>::edge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    // iterate over edges and check for matching links
    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;

        // check, if link does match
        auto current_link = graph[*edge].ptr;
        if (current_link->getSource()->getId() == sourceNode->getId() &&
            current_link->getDestination()->getId() == destNode->getId()) {
            return current_link;
        }
    }

    // no edge matched
    return nullptr;
};

bool ExecutionGraph::hasLink(ExecutionNodePtr sourceNode, ExecutionNodePtr destNode) const {
    // edge found
    if (getLink(sourceNode, destNode) != nullptr) {
        return true;
    }

    // no edge found
    return false;
};

const ExecutionEdge *ExecutionGraph::getEdge(int search_id) const {
    // build edge iterator
    executionEdge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    // iterate over edges
    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;

        // return matching edge
        if (graph[*edge].id == search_id) {
            return &graph[*edge];
        }
    }

    // no matching edge found
    return nullptr;
};

bool ExecutionGraph::hasEdge(int search_id) const {
    // edge found
    if (getEdge(search_id) != nullptr) {
        return true;
    }

    // no edge found
    return false;
};

bool ExecutionGraph::addEdge(ExecutionNodeLinkPtr ptr) {
    // link id is already in graph
    if (hasEdge(ptr->getLinkId())) {
        return false;
    }

    // there is already a link between those two vertices
    if (hasLink(ptr->getSource(), ptr->getDestination())) {
        return false;
    }

    // check if vertices are in graph
    if (!hasVertex(ptr->getSource()->getId()) || !hasVertex(ptr->getDestination()->getId())) {
        return false;
    }

    // add edge with link
    auto src = getVertex(ptr->getSource()->getId());
    auto dst = getVertex(ptr->getDestination()->getId());
    boost::add_edge(src, dst, ExecutionEdge{ptr->getLinkId(), ptr}, graph);

    return true;
};

const std::vector<ExecutionEdge> ExecutionGraph::getAllEdgesToNode(ExecutionNodePtr destNode) const {


    std::vector<ExecutionEdge> result = {};

    executionEdge_iterator edge, edge_end, next_edge;
    boost::tie(edge, edge_end) = edges(graph);

    for (next_edge = edge; edge != edge_end; edge = next_edge) {
        ++next_edge;

        if (graph[*edge].ptr->getDestination()->getId() == destNode.get()->getId()) {
            result.push_back(graph[*edge]);
        }
    }

    return result;
}

std::string ExecutionGraph::getGraphString() {
    std::stringstream ss;
    boost::write_graphviz(ss, graph,
                          [&](auto &out, auto v) {
                              out << "[label=\"" << graph[v].id << " operatorName="
                                  << graph[v].ptr->getOperatorName() << " nodeName="
                                  << graph[v].ptr->getNodeName()
                                  << "\"]";
                          },
                          [&](auto &out, auto e) { out << "[label=\"" << graph[e].id << "\"]"; });
    ss << std::flush;
    return ss.str();
};

OptimizedExecutionGraph::OptimizedExecutionGraph() {
    fGraph = new ExecutionGraph();
}

ExecutionNodePtr OptimizedExecutionGraph::getRootNode() const {
    return fGraph->getRoot();
};

ExecutionNodePtr
OptimizedExecutionGraph::createExecutionNode(std::string operatorName, std::string nodeName,
                                             FogTopologyEntryPtr fogNode,
                                             OperatorPtr executableOperator) {

    ExecutionNode executionNode(operatorName, nodeName, fogNode, executableOperator);

    // create worker node
    auto ptr = std::make_shared<ExecutionNode>(executionNode);
    fGraph->addVertex(ptr);
    return ptr;
};

bool OptimizedExecutionGraph::hasVertex(int search_id) {
    return fGraph->hasVertex(search_id);
}

ExecutionNodePtr OptimizedExecutionGraph::getExecutionNode(int search_id) {
    return fGraph->getNode(search_id);
}

json::value OptimizedExecutionGraph::getExecutionGraphAsJson() const {
    const ExecutionNodePtr &rootNode = getRootNode();

    auto topologyAsJson = json::value::object();

    const auto label = std::to_string(rootNode->getId()) + "-" + rootNode->getOperatorName();
    topologyAsJson["name"] = json::value::string(label);
    auto children = getChildrenNode(rootNode);
    if (!children.empty()) {
        topologyAsJson["children"] = json::value::array(children);
    }

    return topologyAsJson;
}

std::vector<json::value> OptimizedExecutionGraph::getChildrenNode(ExecutionNodePtr executionParentNode) const {


    const ExecutionGraph &executionGraph = getExecutionGraph();

    const std::vector<ExecutionEdge> &edgesToNode = executionGraph.getAllEdgesToNode(executionParentNode);

    std::vector<json::value> children = {};

    if (edgesToNode.empty()) {
        return {};
    }

    for (ExecutionEdge edge: edgesToNode) {
        const ExecutionNodePtr &sourceNode = edge.ptr->getSource();
        if (sourceNode) {
            auto child = json::value::object();
            const auto label = std::to_string(sourceNode->getId()) + "-" + sourceNode->getOperatorName();
            child["name"] = json::value::string(label);
            const std::vector<json::value> &grandChildren = getChildrenNode(sourceNode);
            if (!grandChildren.empty()) {
                child["children"] = json::value::array(grandChildren);
            }
            children.push_back(child);
        }
    }
    return children;
}

ExecutionNodeLinkPtr OptimizedExecutionGraph::createExecutionNodeLink(ExecutionNodePtr src, ExecutionNodePtr dst) {

    // check if link already exists
    if (fGraph->hasLink(src, dst)) {
        // return already existing link
        return fGraph->getLink(src, dst);
    }

    // create new link
    auto linkPtr = std::make_shared<ExecutionNodeLink>(src, dst);
    assert(fGraph->addEdge(linkPtr));
    return linkPtr;
};

std::string OptimizedExecutionGraph::getTopologyPlanString() const {
    return fGraph->getGraphString();
};

ExecutionGraph OptimizedExecutionGraph::getExecutionGraph() const {
    return *fGraph;
};
    