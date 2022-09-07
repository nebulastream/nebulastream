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
#ifndef NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_TOPOLOGYCONTROLLER_HPP_
#define NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_TOPOLOGYCONTROLLER_HPP_
#include <REST/DTOs/TopologyDTO.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <nlohmann/json.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Experimental/NodeTypeUtilities.hpp>
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
     * @param topology
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     * @param errorHandler - responsible for handling errors
     */
    TopologyController(const std::shared_ptr<ObjectMapper>& objectMapper,
                           TopologyPtr topology,
                           oatpp::String completeRouterPrefix,
                           ErrorHandlerPtr errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix),
          topology(topology), errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param topology
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @param errorHandler - responsible for handling errors
     */
    static std::shared_ptr<TopologyController> createShared(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                                TopologyPtr topology,
                                                                std::string routerPrefixAddition,
                                                                ErrorHandlerPtr errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<TopologyController>(objectMapper,
                                                        topology,
                                                        completeRouterPrefix,
                                                        errorHandler);
    }

    ENDPOINT("GET", "", getTopology) {
        auto dto = DTO::TopologyDTO::createShared();
        oatpp::List<oatpp::Object<DTO::QueryCatalogEntryResponse>> list({});
        auto topologyJson = getTopologyAsJson(topology);
        try {
            oatpp::List<oatpp::Object<DTO::Edge>> edges({});
            auto edgesJson = topologyJson["edges"];
            for(auto edge : edgesJson){
                auto edgeDTO = DTO::Edge::createShared();
                edgeDTO->source = edge["source"].get<std::string>();
                edgeDTO->target = edge["target"].get<std::string>();
                edges->push_back(edgeDTO);
            }
            dto->edges = edges;
            //create list of nodes
            oatpp::List<oatpp::Object<DTO::TopologyNode>> nodes({});
            auto nodesJson = topologyJson["nodes"];
            for(auto node : nodesJson){
                auto nodeDTO = DTO::TopologyNode::createShared();
                auto locationDTO = DTO::LocationInformation::createShared();
                auto locationInfoJson = node["location"];
                NES_DEBUG(nodesJson.dump());
                if(!locationInfoJson.empty()) {
                    locationDTO->latitude = locationInfoJson["latitude"].get<std::string>();
                    locationDTO->longitude = locationInfoJson["longitude"].get<std::string>();
                }
                nodeDTO->availableResources = node["available_resources"].get<uint64_t>();
                nodeDTO->id = node["id"].get<uint64_t>();
                nodeDTO->locationInformation = locationDTO;
                nodeDTO->nodeType = node["nodeType"].get<std::string>();
                nodes->push_back(nodeDTO);
            }
            dto->nodes = nodes;
            return createDtoResponse(Status::CODE_200, dto);
        } catch (const std::exception& exc) {
            NES_ERROR("TopologyController: handleGet -getTopology: Exception occurred while building the "
                      "topology:"
                      << exc.what());
            return errorHandler->handleError(Status::CODE_500,
                                             "Exception occurred while building topology"
                                                 + std::string(exc.what()));
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

  private:
    /**
      * @brief function to obtain JSON representation of a NES Topology
      * @param root of the Topology
      * @return JSON representation of the Topology
      */
    nlohmann::json getTopologyAsJson(TopologyPtr topology){
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
                NES::Spatial::Index::Experimental::Location location = currentNode->getCoordinates();
                auto locationInfo = nlohmann::json{};
                if (location.isValid()) {
                    locationInfo["latitude"] = location.getLatitude();
                    locationInfo["longitude"] = location.getLongitude();
                }
                currentNodeJsonValue["location"] = locationInfo;
            }
            currentNodeJsonValue["nodeType"] =
                NES::Spatial::Util::NodeTypeUtilities::toString(currentNode->getSpatialNodeType());

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
#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_TOPOLOGYCONTROLLER_HPP_
