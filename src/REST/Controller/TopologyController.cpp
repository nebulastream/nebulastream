#include <Components/NesCoordinator.hpp>
#include <REST/Controller/TopologyController.hpp>
#include <REST/std_service.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>

using namespace web;
using namespace http;

namespace NES {

TopologyController::TopologyController(TopologyPtr topology)
    : topology(topology) {}

void TopologyController::handleGet(std::vector<utility::string_t> path, http_request message) {

    TopologyNodePtr rootNode = topology->getRoot();

    web::json::value topologyJson{};

    std::deque<TopologyNodePtr> parentToPrint{rootNode};
    std::deque<TopologyNodePtr> childToPrint;

    std::vector<web::json::value> nodes = {};
    std::vector<web::json::value> edges = {};


    while (!parentToPrint.empty()) {
        TopologyNodePtr nodeToPrint = parentToPrint.front();
        web::json::value currentNodeJsonValue{};


        parentToPrint.pop_front();
        currentNodeJsonValue["id"] = web::json::value::number(nodeToPrint->getId());
        currentNodeJsonValue["available_resources"] = web::json::value::number(nodeToPrint->getAvailableResources());
        currentNodeJsonValue["ip_address"] = web::json::value::string(nodeToPrint->getIpAddress());

        for (auto& child : nodeToPrint->getChildren()) {
            web::json::value currentEdgeJsonValue{};
            currentEdgeJsonValue["source"] = web::json::value::number(child->as<TopologyNode>()->getId());
            currentEdgeJsonValue["target"] = web::json::value::number(nodeToPrint->getId());
            edges.push_back(currentEdgeJsonValue);

            childToPrint.push_back(child->as<TopologyNode>());
        }

        if (parentToPrint.empty()) {
            parentToPrint.insert(parentToPrint.end(), childToPrint.begin(), childToPrint.end());
            childToPrint.clear();
        }

        nodes.push_back(currentNodeJsonValue);

    }

    topologyJson["nodes"] = web::json::value::array(nodes);
    topologyJson["edges"] = web::json::value::array(edges);

    successMessageImpl(message, topologyJson);
    return;
}

}// namespace NES
