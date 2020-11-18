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

}// namespace NES
