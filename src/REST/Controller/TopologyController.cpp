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
#include <REST/std_service.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/UtilityFunctions.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>

using namespace web;
using namespace http;

namespace NES {

TopologyController::TopologyController(TopologyPtr topology, NesCoordinatorWeakPtr coordinator) : topology(topology),
                                                                                                  coordinator(coordinator)  {}

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
void TopologyController::handlePost(std::vector<utility::string_t> path, web::http::http_request message) {
    if (path[1] == "add-parent") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
              try {
                  std::string userRequest(body.begin(), body.end());

                  json::value req = json::value::parse(userRequest);
                  json::array links = req.at("links").as_array();
                  // for each link in the request body: parse the child and parent then connect using add parent
                  bool isSuccess = true;
                  for (int i=0; i<links.size(); i++){
                      json::value link = links.at(i);
                      // assume there is 1:1 mapping between IP and ID
                      uint64_t parentId = topology->findNodeWithIp(link.at("parent").as_string())[0]->getId();
                      uint64_t childId = topology->findNodeWithIp(link.at("child").as_string())[0]->getId();

                      bool added = coordinator.lock()->getCoordinatorEngine()->addParent(childId,parentId);
                      isSuccess = isSuccess && added;
                  }

                  //Prepare the response
                  json::value result{};
                  result["Success"] = json::value::boolean(isSuccess);
                  successMessageImpl(message, result);
                  return;
              } catch (...) {
                  std::cout << "Exception occurred adding parent.";
                  internalServerErrorImpl(message);
                  return;
              }
            }).wait();
    } else if (path[1] == "remove-parent") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
              try {
                  std::string userRequest(body.begin(), body.end());

                  json::value req = json::value::parse(userRequest);
                  json::array links = req.at("links").as_array();
                  // for each link in the request body: parse the child and parent then connect using add parent
                  bool isSuccess = true;
                  for (int i=0; i<links.size(); i++){
                      json::value link = links.at(i);
                      // assume there is 1:1 mapping between IP and ID
                      uint64_t parentId = topology->findNodeWithIp(link.at("parent").as_string())[0]->getId();
                      uint64_t childId = topology->findNodeWithIp(link.at("child").as_string())[0]->getId();

                      bool removed = coordinator.lock()->getCoordinatorEngine()->removeParent(childId,parentId);
                      isSuccess = isSuccess && removed;
                  }

                  //Prepare the response
                  json::value result{};
                  result["Success"] = json::value::boolean(isSuccess);
                  successMessageImpl(message, result);
                  return;
              } catch (...) {
                  std::cout << "Exception occurred removing parent.";
                  internalServerErrorImpl(message);
                  return;
              }
            }).wait();
    }
}

}// namespace NES
