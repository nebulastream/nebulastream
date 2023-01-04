/*
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
#ifndef NES_CORE_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_
#define NES_CORE_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Experimental/NodeTypeUtilities.hpp>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST {
namespace Controller {
class TopologyController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param topology - the overall physical infrastructure with different nodes
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     * @param errorHandler - responsible for handling errors
     */
    TopologyController(const std::shared_ptr<ObjectMapper>& objectMapper,
                       TopologyPtr topology,
                       oatpp::String completeRouterPrefix,
                       ErrorHandlerPtr errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), topology(std::move(topology)),
          errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param topology - the overall physical infrastructure with different nodes
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @param errorHandler - responsible for handling errors
     */
    static std::shared_ptr<TopologyController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                      TopologyPtr topology,
                                                      std::string routerPrefixAddition,
                                                      ErrorHandlerPtr errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<TopologyController>(objectMapper, std::move(topology), completeRouterPrefix, errorHandler);
    }

    ENDPOINT("GET", "", getTopology) {
        try {
            auto topologyJson = getTopologyAsJson(topology);
            return createResponse(Status::CODE_200, topologyJson.dump());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (const std::exception& exc) {
            NES_ERROR("TopologyController: handleGet -getTopology: Exception occurred while building the "
                      "topology:"
                      << exc.what());
            return errorHandler->handleError(Status::CODE_500,
                                             "Exception occurred while building topology" + std::string(exc.what()));
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("POST", "/addParent", addParent, BODY_STRING(String, request)) {
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            };
            nlohmann::json reqJson = nlohmann::json::parse(req);
            auto optional = validateRequest(reqJson);
            if (optional.has_value()) {
                return optional.value();
            }
            uint64_t parentId = reqJson["parentId"].get<uint64_t>();
            uint64_t childId = reqJson["childId"].get<uint64_t>();
            auto parentPhysicalNode = topology->findNodeWithId(parentId);
            auto childPhysicalNode = topology->findNodeWithId(childId);

            bool added = topology->addNewTopologyNodeAsChild(parentPhysicalNode, childPhysicalNode);
            if (added) {
                NES_DEBUG("TopologyController::handlePost:addParent: created link successfully new topology is=");
                topology->print();
            } else {
                NES_ERROR("TopologyController::handlePost:addParent: Failed");
                return errorHandler->handleError(Status::CODE_500, "TopologyController::handlePost:addParent: Failed");
            }
            //Prepare the response
            nlohmann::json response;
            response["success"] = added;
            return createResponse(Status::CODE_200, response.dump());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server Error");
        }
    }

    ENDPOINT("DELETE", "/removeParent", removeParent, BODY_STRING(String, request)) {
        try {
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            };
            nlohmann::json reqJson = nlohmann::json::parse(req);
            auto optional = validateRequest(reqJson);
            if (optional.has_value()) {
                return optional.value();
            }
            uint64_t parentId = reqJson["parentId"].get<uint64_t>();
            uint64_t childId = reqJson["childId"].get<uint64_t>();
            auto parentPhysicalNode = topology->findNodeWithId(parentId);
            auto childPhysicalNode = topology->findNodeWithId(childId);

            bool removed = topology->removeNodeAsChild(parentPhysicalNode, childPhysicalNode);
            if (removed) {
                NES_DEBUG("TopologyController::handlePost:addParent: deleted link successfully");
            } else {
                NES_ERROR("TopologyController::handlePost:addParent: Failed");
                return errorHandler->handleError(Status::CODE_500, "TopologyController::handlePost:removeParent: Failed");
            }
            //Prepare the response
            nlohmann::json response;
            response["success"] = removed;
            return createResponse(Status::CODE_200, response.dump());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server Error");
        }
    }

  private:
    std::optional<std::shared_ptr<oatpp::web::protocol::http::outgoing::Response>> validateRequest(nlohmann::json reqJson) {
        if (reqJson.empty()) {
            return errorHandler->handleError(Status::CODE_400, "empty body");
        }
        if (!reqJson.contains("parentId")) {
            return errorHandler->handleError(Status::CODE_400, " Request body missing 'parentId'");
        }
        if (!reqJson.contains("childId")) {
            return errorHandler->handleError(Status::CODE_400, " Request body missing 'childId'");
        }
        uint64_t parentId = reqJson["parentId"].get<uint64_t>();
        uint64_t childId = reqJson["childId"].get<uint64_t>();
        if (parentId == childId) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add parent for node in topology: childId and parentId must be different.");
        }

        TopologyNodePtr childPhysicalNode = topology->findNodeWithId(childId);
        if (!childPhysicalNode) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add parent for node in topology: Node with childId=" + std::to_string(childId) + " not found.");
        }

        TopologyNodePtr parentPhysicalNode = topology->findNodeWithId(parentId);
        if (!parentPhysicalNode) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add parent for node in topology: Node with parentId=" + std::to_string(parentId) + " not found.");
        }
        return std::nullopt;
    }

    /**
      * @brief function to obtain JSON representation of a NES Topology
      * @param root of the Topology
      * @return JSON representation of the Topology
      */
    nlohmann::json getTopologyAsJson(TopologyPtr topology) {
        NES_INFO("TopologyController: getting topology as JSON");

        nlohmann::json topologyJson{};
        auto root = topology->getRoot();
        std::deque<TopologyNodePtr> parentToAdd{std::move(root)};
        std::deque<TopologyNodePtr> childToAdd;

        std::vector<nlohmann::json> nodes = {};
        std::vector<nlohmann::json> edges = {};

        while (!parentToAdd.empty()) {
            // Current topology node to add to the JSON
            TopologyNodePtr currentNode = parentToAdd.front();
            nlohmann::json currentNodeJsonValue{};

            parentToAdd.pop_front();
            // Add properties for current topology node
            currentNodeJsonValue["id"] = currentNode->getId();
            currentNodeJsonValue["available_resources"] = currentNode->getAvailableResources();
            currentNodeJsonValue["ip_address"] = currentNode->getIpAddress();
            if (currentNode->getSpatialNodeType() != NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE) {
                NES::Spatial::Index::Experimental::Location location = *currentNode->getCoordinates()->getLocation();
                auto locationInfo = nlohmann::json{};
                if (location.isValid()) {
                    locationInfo["latitude"] = location.getLatitude();
                    locationInfo["longitude"] = location.getLongitude();
                }
                currentNodeJsonValue["location"] = locationInfo;
            }
            currentNodeJsonValue["nodeType"] = NES::Spatial::Util::NodeTypeUtilities::toString(currentNode->getSpatialNodeType());

            for (const auto& child : currentNode->getChildren()) {
                // Add edge information for current topology node
                nlohmann::json currentEdgeJsonValue{};
                currentEdgeJsonValue["source"] = child->as<TopologyNode>()->getId();
                currentEdgeJsonValue["target"] = currentNode->getId();
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
        topologyJson["nodes"] = nodes;
        topologyJson["edges"] = edges;
        return topologyJson;
    }

    TopologyPtr topology;
    ErrorHandlerPtr errorHandler;
};
}//namespace Controller
}// namespace REST
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)
#endif// NES_CORE_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_
