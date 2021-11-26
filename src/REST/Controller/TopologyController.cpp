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
#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/http_msg.h>
#include <cpprest/json.h>
#include <utility>
#include <vector>

namespace NES {

TopologyController::TopologyController(TopologyPtr topology) : topology(std::move(topology)) {}

void TopologyController::handleGet(const std::vector<utility::string_t>& paths, web::http::http_request& message) {
    NES_DEBUG("TopologyController: GET Topology");

    //TODO: ignore nodes marked for maintenance
    topology->print();
    if (paths.size() == 1) {
        web::json::value topologyJson = getTopologyAsJson(topology);
        successMessageImpl(message, topologyJson);
        return;
    }
    resourceNotFoundImpl(message);
}

void TopologyController::handlePost(const std::vector<utility::string_t>& path, web::http::http_request& message) {
    if (path[1] == "addParent") {
        NES_DEBUG("TopologyController::handlePost:addParent: REST received request to add parent for a node"
                  << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("TopologyController::handlePost:addParent: Start trying to add new parent for a node");
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    NES_DEBUG("TopologyController::handlePost:addParent: userRequest: " << payload);
                    web::json::value req = web::json::value::parse(payload);
                    NES_DEBUG("TopologyController::handlePost:addParent: Json Parse Value: " << req);
                    uint64_t childId = std::stoull(req.at("childId").as_string());
                    uint64_t parentId = std::stoull(req.at("parentId").as_string());

                    NES_DEBUG("TopologyController::handlePost:addParent: childId=" << childId << " parentId=" << parentId);

                    if (parentId == childId) {
                        throw Exception("Could not add parent for node in topology: childId and parentId must be different.");
                    }

                    TopologyNodePtr childPhysicalNode = topology->findNodeWithId(childId);
                    if (!childPhysicalNode) {
                        throw Exception("Could not add parent for node in topology: Node with childId=" + std::to_string(childId)
                                        + " not found.");
                    }

                    TopologyNodePtr parentPhysicalNode = topology->findNodeWithId(parentId);
                    if (!parentPhysicalNode) {
                        throw Exception("Could not add parent for node in topology: Node with parentId="
                                        + std::to_string(parentId) + " not found.");
                    }


bool added = topology->addNewPhysicalNodeAsChild(parentPhysicalNode, childPhysicalNode);
                    if (added) {
                        NES_DEBUG("TopologyController::handlePost:addParent: created link successfully new topology is=");
                        topology->print();
                    } else {
                        NES_ERROR("TopologyController::handlePost:addParent: Failed");
                        throw Exception("TopologyController::handlePost:addParent: Failed");
                    }

                    //Prepare the response
                    web::json::value result{};
                    result["Success"] = web::json::value::boolean(added);
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "TopologyController::handlePost:addParent: Exception occurred while trying to add parent for a node "
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
    } else if (path[1] == "removeParent") {

        NES_DEBUG("TopologyController::handlePost:removeParent: REST received request to remove parent for a node"
                  << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("TopologyController::handlePost:removeParent: Start trying to remove parent for a node");
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    NES_DEBUG("TopologyController::handlePost:removeParent: userRequest: " << payload);
                    web::json::value req = web::json::value::parse(payload);
                    NES_DEBUG("TopologyController::handlePost:removeParent: Json Parse Value: " << req);
                    uint64_t childId = std::stoull(req.at("childId").as_string());
                    uint64_t parentId = std::stoull(req.at("parentId").as_string());

                    NES_DEBUG("TopologyController::handlePost:removeParent: childId=" << childId << " parentId=" << parentId);

                    if (parentId == childId) {
                        throw Exception("Could not remove parent for node in topology: childId and parentId must be different.");
                    }

                    TopologyNodePtr childPhysicalNode = topology->findNodeWithId(childId);
                    if (!childPhysicalNode) {
                        throw Exception("Could not remove parent for node in topology: Node with childId="
                                        + std::to_string(childId) + " not found.");
                    }
                    NES_DEBUG("TopologyController::handlePost:removeParent: childId: " << childId << " exists");

                    TopologyNodePtr parentPhysicalNode = topology->findNodeWithId(parentId);
                    if (!parentPhysicalNode) {
                        throw Exception("Could not remove parent for node in topology: Node with parentId="
                                        + std::to_string(parentId) + " not found.");
                    }
                    NES_DEBUG("TopologyController::handlePost:removeParent: parent node: " << parentId << " exists");

                    bool added = topology->removeNodeAsChild(parentPhysicalNode, childPhysicalNode);
                    if (added) {
                        NES_DEBUG("TopologyController::handlePost:removeParent: removed link successfully new topology is=");
                        topology->print();
                    } else {
                        NES_ERROR("TopologyController::handlePost:removeParent: Failed");
                        throw Exception("TopologyController::handlePost:removeParent: Failed");
                    }

                    //Prepare the response
                    web::json::value result{};
                    result["Success"] = web::json::value::boolean(added);
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("TopologyController::handlePost:removeParent: Exception occurred while trying to remove parent."
                              "for a node "
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
    } else if (path[1] == "unmark") {

        NES_DEBUG("TopologyController: handlePost -mark: REST received request to mark node for maintenance "
                  << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
              try {
                  NES_DEBUG("TopologyController: handlePost -unmark: Start trying to mark nodes for maintenance");
                  //Parse IDs of nodes to mark for maintenance
                  std::string payload(body.begin(), body.end());
                  NES_DEBUG("TopologyController: handlePost -unmark: userRequest: " << payload);
                  json::value req = json::value::parse(payload);
                  NES_DEBUG("TopologyController: handlePost -unmark: Json Parse Value: " << req);
                  //TODO: handle multiple IDs

                    uint64_t id = req.at("id").as_integer();

                    auto node = topology->findNodeWithId(id);

                    bool checkFlag;


                    NES_DEBUG("TopologyController: handlePost -mark: Unmark node for maintenance: "
                                    << id);

                    node->setMaintenanceFlag(false);
                    checkFlag = node->getMaintenanceFlag();
                    NES_DEBUG("TopologyController: handlePost -unmark: Successfully unmarked node ?"
                                    << !checkFlag);
                      //Prepare the response

                  json::value result{};
                  result["Successful unmark"] = json::value::boolean(!checkFlag);
                  result["Node Id"]      =json::value::number(id);

                  successMessageImpl(message, result);
                  return;

                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "TopologyController: handlePost -mark: Exception occurred while trying to mark node for maintenance "
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
    } else {
        resourceNotFoundImpl(message);
    }
    return;
}

web::json::value TopologyController::getTopologyAsJson(TopologyPtr topo) {
    NES_INFO("TopologyController: getting topology as JSON");

    web::json::value topologyJson{};
    auto root = topo->getRoot();
    std::deque<TopologyNodePtr> parentToAdd{std::move(root)};
    std::deque<TopologyNodePtr> childToAdd;

    std::vector<web::json::value> nodes = {};
    std::vector<web::json::value> edges = {};

    while (!parentToAdd.empty()) {
        // Current topology node to add to the JSON
        TopologyNodePtr currentNode = parentToAdd.front();
        web::json::value currentNodeJsonValue{};

        parentToAdd.pop_front();
        // Add properties for current topology node
        currentNodeJsonValue["id"] = web::json::value::number(currentNode->getId());
        currentNodeJsonValue["available_resources"] = web::json::value::number(currentNode->getAvailableResources());
        currentNodeJsonValue["ip_address"] = web::json::value::string(currentNode->getIpAddress());

        for (const auto& child : currentNode->getChildren()) {
            // Add edge information for current topology node
            web::json::value currentEdgeJsonValue{};
            currentEdgeJsonValue["source"] = web::json::value::number(child->as<TopologyNode>()->getId());
            currentEdgeJsonValue["target"] = web::json::value::number(currentNode->getId());
            edges.push_back(currentEdgeJsonValue);

            childToAdd.push_back(child->as<TopologyNode>());
        }

        if (parentToAdd.empty()) {
            parentToAdd.insert(parentToAdd.end(), childToAdd.begin(), childToAdd.end());
            childToAdd.clear();
        }

        nodes.push_back(currentNodeJsonValue);
    }
    NES_INFO("TopologyController: no more topology node to add");

    // add `nodes` and `edges` JSON array to the final JSON result
    topologyJson["nodes"] = web::json::value::array(nodes);
    topologyJson["edges"] = web::json::value::array(edges);
    return topologyJson;
}
}// namespace NES
