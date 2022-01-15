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
#include <GRPC/WorkerRPCClient.hpp>
#include <REST/Controller/DumpFileController.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <REST/Controller/TopologyController.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/http_msg.h>
#include <utility>
#include <vector>

namespace NES {

DumpFileController::DumpFileController(TopologyPtr topology, WorkerRPCClientPtr workerClient)
    : topology(std::move(topology)), workerClient(std::move(workerClient)) {
    NES_DEBUG("Created DumpFileController");
}

void DumpFileController::handleGet(const std::vector<utility::string_t>& paths, web::http::http_request& message) {
    NES_DEBUG("TopologyController: GET DumpFiles");

    if (paths[1] == "node") {
        auto strNodeId = paths[2];
        if (!strNodeId.empty() && std::all_of(strNodeId.begin(), strNodeId.end(), ::isdigit)) {
            // call node
            uint64_t nodeId = std::stoi(strNodeId);
            TopologyNodePtr node = topology->findNodeWithId(nodeId);

            auto nodeIp = node->getIpAddress();
            auto nodeGrpcPort = node->getGrpcPort();
            std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);
            std::string jsonAsString;

            auto success = workerClient->getDumpInfoFromNode(destAddress, &jsonAsString);

            if (success) {
                NES_DEBUG("DumpFileController: Received dump files with status " + std::to_string(success));
                successMessageImpl(message, jsonAsString);
                return;
            }
        }
        // else bad request
    }
    resourceNotFoundImpl(message);
}
}
