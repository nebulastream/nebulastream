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

void TopologyController::handleGet(std::vector<utility::string_t> paths, http_request message) {
    NES_DEBUG("TopologyController: GET Topology");

    if (paths.size()==1){
        web::json::value topologyJson = UtilityFunctions::getTopologyAsJson(topology->getRoot());
        successMessageImpl(message, topologyJson);
        return;
    }
    resourceNotFoundImpl(message);
    return;
}

}// namespace NES
