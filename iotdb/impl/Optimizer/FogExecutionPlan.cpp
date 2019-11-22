
#include <Optimizer/FogExecutionPlan.hpp>
#include <boost/graph/graphviz.hpp>
#include <cpprest/json.h>
#include <Topology/FogTopologySensorNode.hpp>
#include <set>

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

vector<ExecutionVertex> ExecutionGraph::getAllVertex() const {

  vector<ExecutionVertex> result = {};

  executionVertex_iterator vertex, vertex_end, next_vertex;
  boost::tie(vertex, vertex_end) = vertices(graph);

  // iterator over vertices
  for (next_vertex = vertex; vertex != vertex_end; vertex = next_vertex) {
    ++next_vertex;
    result.push_back(graph[*vertex]);
  }

  return result;
}

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

vector<ExecutionEdge> ExecutionGraph::getAllEdges() const {

  vector<ExecutionEdge> result = {};

  executionEdge_iterator edge, edge_end, next_edge;
  boost::tie(edge, edge_end) = edges(graph);

  // iterate over edges
  for (next_edge = edge; edge != edge_end; edge = next_edge) {
    ++next_edge;
    result.push_back(graph[*edge]);
  }

  return result;
}

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

const vector<ExecutionEdge> ExecutionGraph::getAllEdgesFromNode(iotdb::ExecutionNodePtr srcNode) const {
  std::vector<ExecutionEdge> result = {};

  executionEdge_iterator edge, edge_end, next_edge;
  boost::tie(edge, edge_end) = edges(graph);

  for (next_edge = edge; edge != edge_end; edge = next_edge) {
    ++next_edge;

    if (graph[*edge].ptr->getSource()->getId() == srcNode.get()->getId()) {
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
}

FogExecutionPlan::FogExecutionPlan() {
  exeGraphPtr = std::make_shared<ExecutionGraph>();
}

ExecutionNodePtr FogExecutionPlan::getRootNode() const {
  return exeGraphPtr->getRoot();
};

ExecutionNodePtr
FogExecutionPlan::createExecutionNode(std::string operatorName, std::string nodeName,
                                      FogTopologyEntryPtr fogNode,
                                      OperatorPtr executableOperator) {

  ExecutionNode executionNode(operatorName, nodeName, fogNode, executableOperator);

  // create worker node
  auto ptr = std::make_shared<ExecutionNode>(executionNode);
  exeGraphPtr->addVertex(ptr);
  return ptr;
};

bool FogExecutionPlan::hasVertex(int search_id) {
  return exeGraphPtr->hasVertex(search_id);
}

ExecutionNodePtr FogExecutionPlan::getExecutionNode(int search_id) {
  return exeGraphPtr->getNode(search_id);
}

json::value FogExecutionPlan::getExecutionGraphAsJson() const {
  const ExecutionNodePtr &rootNode = getRootNode();

  const shared_ptr<ExecutionGraph> &exeGraph = getExecutionGraph();

  const vector<ExecutionEdge> &allEdges = exeGraph->getAllEdges();
  const vector<ExecutionVertex> &allVertex = exeGraph->getAllVertex();

  auto result = json::value::object();
  std::vector<json::value> edges{};
  std::vector<json::value> vertices{};
  for (u_int i = 0; i < allEdges.size(); i++) {
    const ExecutionEdge &edge = allEdges[i];
    const ExecutionNodePtr &sourceNode = edge.ptr->getSource();
    const ExecutionNodePtr &destNode = edge.ptr->getDestination();
    auto edgeInfo = json::value::object();
    const auto source = "Node-" + std::to_string(sourceNode->getId());
    const auto dest = "Node-" + std::to_string(destNode->getId());
    edgeInfo["source"] = json::value::string(source);
    edgeInfo["target"] = json::value::string(dest);
    edges.push_back(edgeInfo);
  }

  for (u_int i = 0; i < allVertex.size(); i++) {
    const ExecutionVertex &vertex = allVertex[i];
    auto vertexInfo = json::value::object();
    const ExecutionNodePtr &executionNodePtr = vertex.ptr;
    const string id = "Node-" + std::to_string(executionNodePtr->getId());
    const string &operatorName = executionNodePtr->getOperatorName();
    const string nodeType = executionNodePtr->getFogNode()->getEntryTypeString();

    int cpuCapacity = vertex.ptr->getFogNode()->getCpuCapacity();
    int remainingCapacity = vertex.ptr->getFogNode()->getRemainingCpuCapacity();

    vertexInfo["id"] = json::value::string(id);
    vertexInfo["capacity"] = json::value::string(std::to_string(cpuCapacity));
    vertexInfo["remainingCapacity"] = json::value::string(std::to_string(remainingCapacity));
    vertexInfo["operators"] = json::value::string(operatorName);
    vertexInfo["nodeType"] = json::value::string(nodeType);
    if (nodeType == "Sensor") {
      FogTopologySensorNodePtr ptr = std::static_pointer_cast<FogTopologySensorNode>(vertex.ptr->getFogNode());
      vertexInfo["sensorType"] = json::value::string(ptr->getSensorType());
    }
    vertices.push_back(vertexInfo);
  }

  result["nodes"] = json::value::array(vertices);
  result["edges"] = json::value::array(edges);
  return result;
}

std::vector<json::value> FogExecutionPlan::getChildrenNode(ExecutionNodePtr executionParentNode) const {

  const shared_ptr<ExecutionGraph> &exeGraph = getExecutionGraph();

  const std::vector<ExecutionEdge> &edgesToNode = exeGraph->getAllEdgesToNode(executionParentNode);

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

ExecutionNodeLinkPtr FogExecutionPlan::createExecutionNodeLink(ExecutionNodePtr src, ExecutionNodePtr dst) {

  // check if link already exists
  if (exeGraphPtr->hasLink(src, dst)) {
    // return already existing link
    return exeGraphPtr->getLink(src, dst);
  }

  // create new link
  auto linkPtr = std::make_shared<ExecutionNodeLink>(src, dst);
  assert(exeGraphPtr->addEdge(linkPtr));
  return linkPtr;
};

std::shared_ptr<ExecutionGraph> FogExecutionPlan::getExecutionGraph() const {
  return exeGraphPtr;
};

void FogExecutionPlan::freeResources(int freedCapacity) {
  for (const ExecutionVertex &v: getExecutionGraph()->getAllVertex()) {
    if (v.ptr->getRootOperator()) {
      v.ptr->getFogNode()->increaseCpuCapacity(freedCapacity);
    }
  }
}

std::string FogExecutionPlan::getTopologyPlanString() const {
  return exeGraphPtr->getGraphString();
}