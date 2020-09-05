#include <Components/NesCoordinator.hpp>
#include <REST/Controller/TopologyController.hpp>
#include <REST/std_service.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/UtilityFunctions.hpp>

using namespace web;
using namespace http;

namespace NES {

TopologyController::TopologyController(TopologyPtr topology)
    : topology(topology) {}

void TopologyController::handleGet(std::vector<utility::string_t> path, http_request message) {

    web::json::value topologyJson = UtilityFunctions::getTopologyAsJson(topology->getRoot());

    successMessageImpl(message, topologyJson);
    return;
}

}// namespace NES
