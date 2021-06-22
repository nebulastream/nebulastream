/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Components/NesCoordinator.hpp>
#include <REST/Controller/TopologyController.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/UtilityFunctions.hpp>
#include <vector>
#include <REST/runtime_utils.hpp>
using namespace web;
using namespace http;

namespace NES {

TopologyController::TopologyController(TopologyPtr topology) : topology(topology) {}

void TopologyController::handleGet(std::vector<utility::string_t> paths, http_request message) {
    NES_DEBUG("TopologyController: GET Topology");

    topology->print();
    if (paths.size() == 1) {
        web::json::value topologyJson = UtilityFunctions::getTopologyAsJson(topology->getRoot());
        successMessageImpl(message, topologyJson);
        return;
    }
    resourceNotFoundImpl(message);
    return;
}
    // BDAPRO add parameters for ip adress, ports, and resources
    // return json for bad request and success!
    //  return hash id for physicalNode
void TopologyController::handlePost(std::vector<utility::string_t> path, web::http::http_request message) {
    if (path[1] == "createTopologyNode") {
        NES_DEBUG("TopologyController: handlePost -createTopologyNode: REST received request to add new topology node"
                          << message.to_string());
        message.extract_string(true)
                .then([this, message](utility::string_t body) {
                    try {
                        NES_DEBUG("TopologyController: handlePost -createTopologyNode: Start trying to add new topology node");
                        //Prepare Input query from user string
                        std::string payload(body.begin(), body.end());
                        NES_DEBUG("TopologyController: handlePost -createTopologyNode: userRequest: " << payload);
                        json::value req = json::value::parse(payload);
                        NES_DEBUG("TopologyController: handlePost -createTopologyNode: Json Parse Value: " << req);
                        uint64_t nodeId = UtilityFunctions::getNextTopologyNodeId();
                        std::string ipAddress = req.at("ipAddress").as_string();
                        uint32_t grpcPort = req.at("grpcPort").as_integer();
                        uint32_t dataPort = req.at("dataPort").as_integer();
                        uint16_t resources = req.at("resources").as_integer();
                        NES_DEBUG("TopologyController: handlePost -createTopologyNode: Try to add new topology node "
                                          << ipAddress << " and" << grpcPort << " and " << dataPort << " and " << resources);
                        TopologyPtr topology = Topology::create();
                        // BDAPRO get hashID generator?
                        TopologyNodePtr physicalNode = TopologyNode::create(nodeId, ipAddress, grpcPort, dataPort, resources);
                        NES_DEBUG("TopologyController: handlePost -createTopologyNode: Successfully created new toplogy node with nodeId: "
                                          << physicalNode->getId());
                        //Prepare the response
                        json::value result{};
                        result["nodeId"] = json::value::number(physicalNode->getId());
                        successMessageImpl(message, result);
                        return;
                    }
                    catch (const std::exception& exc) {
                NES_ERROR("TopologyController: handlePost -createTopologyNode: Exception occurred while trying to add new "
                          "topology node"
                                  << exc.what());
                handleException(message, exc);
                return;
            } catch (...) {
                RuntimeUtils::printStackTrace();
                internalServerErrorImpl(message);
                return;
            }
        })
            .wait();





    } else{
        resourceNotFoundImpl(message);
    }
}

}// namespace NES
