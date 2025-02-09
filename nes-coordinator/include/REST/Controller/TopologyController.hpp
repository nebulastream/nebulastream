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
#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_

#include <Catalogs/Topology/Topology.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>

#include OATPP_CODEGEN_BEGIN(ApiController)

#include <Catalogs/Topology/TopologyNode.hpp>
#include <Exceptions/RpcException.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPAddLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPEvents/ISQPRemoveLinkEvent.hpp>
#include <RequestProcessor/RequestTypes/ISQP/ISQPRequest.hpp>

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST::Controller {
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
                       const TopologyPtr& topology,
                       const oatpp::String& completeRouterPrefix,
                       const ErrorHandlerPtr& errorHandler,
                       const RequestHandlerServicePtr requestHandlerService,
                       const CoordinatorConfigurationPtr coordinatorConfiguration, const SourceCatalogPtr sourceCatalog)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), topology(topology),
          errorHandler(errorHandler), requestHandlerService(requestHandlerService), workerRPCClient(WorkerRPCClient::create()),
          coordinatorConfiguration(coordinatorConfiguration), sourceCatalog(sourceCatalog) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param topology - the overall physical infrastructure with different nodes
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @param errorHandler - responsible for handling errors
     */
    static std::shared_ptr<TopologyController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                      const TopologyPtr& topology,
                                                      const std::string& routerPrefixAddition,
                                                      const ErrorHandlerPtr& errorHandler,
                                                      const RequestHandlerServicePtr requestHandlerService,
                                                      CoordinatorConfigurationPtr coordinatorConfiguration, SourceCatalogPtr sourceCatalog) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<TopologyController>(objectMapper,
                                                    std::move(topology),
                                                    completeRouterPrefix,
                                                    errorHandler,
                                                    requestHandlerService,
                                                    coordinatorConfiguration, sourceCatalog);
    }

    ENDPOINT("GET", "", getTopology) {
        try {
            auto topologyJson = topology->toJson();
            return createResponse(Status::CODE_200, topologyJson.dump());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (const std::exception& exc) {
            NES_ERROR("TopologyController: handleGet -getTopology: Exception occurred while building the "
                      "topology: {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_500,
                                             "Exception occurred while building topology" + std::string(exc.what()));
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("POST", "/addAsChild", addParent, BODY_STRING(String, request)) {
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

            auto parentId = reqJson["parentId"].get<WorkerId>();
            auto childId = reqJson["childId"].get<WorkerId>();

            auto children = topology->getChildTopologyNodeIds(parentId);
            for (const auto& child : children) {
                if (child == childId) {
                    return errorHandler->handleError(Status::CODE_400,
                                                     fmt::format("Could not add parent for node in topology: Node with "
                                                                 "childId={} is already a child of node with parentID={}.",
                                                                 childId,
                                                                 parentId));
                }
            }
            bool added = topology->addTopologyNodeAsChild(parentId, childId);
            if (added) {
                NES_DEBUG("TopologyController::handlePost:addParent: created link successfully new topology is=");
            } else {
                NES_ERROR("TopologyController::handlePost:addParent: Failed");
                return errorHandler->handleError(
                    Status::CODE_500,
                    "TopologyController::handlePost:addParent: Failed to add link between parent and child nodes.");
            }

            uint64_t bandwidth = 0;
            if (reqJson.contains("bandwidth")) {
                bandwidth = reqJson["bandwidth"].get<uint64_t>();
            }

            uint64_t latency = 0;
            if (reqJson.contains("latency")) {
                latency = reqJson["latency"].get<uint64_t>();
            }

            bool success = topology->addLinkProperty(parentId, childId, bandwidth, latency);
            if (success) {
                NES_DEBUG("TopologyController::handlePost:addParent: added link property for the link between the parent {} and "
                          "child {} nodes.",
                          parentId,
                          childId);
            } else {
                NES_ERROR("TopologyController::handlePost:addParent: Failed");
                return errorHandler->handleError(Status::CODE_500,
                                                 "TopologyController::handlePost:addParent: Failed to add link property.");
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

    ENDPOINT("POST", "/update", update, BODY_STRING(String, request)) {
        try {
            reconnectCounter++;
            NES_DEBUG("Processing topology update")
            std::string req = request.getValue("{}");
            //check if json is valid
            if (!nlohmann::json::accept(req)) {
                NES_ERROR("Invalid json")
                return errorHandler->handleError(Status::CODE_400, "Invalid JSON");
            };
            NES_DEBUG("Parse json")
            nlohmann::json reqJson = nlohmann::json::parse(req);
            NES_DEBUG("{}", reqJson.dump());
            auto completionQueue = std::make_shared<CompletionQueue>();
            auto bufferJson = reqJson["events"];
            NES_DEBUG("number of events {}", bufferJson.size())
            //startBufferingOnAllSources(reqJson, completionQueue);
            startBufferingOnAllSources(bufferJson, completionQueue);
            NES_DEBUG("Sent buffering messges666666666666666666666666666666666666666666666666666666")
            //todo: check if proactive is enabled here
            std::vector<RequestProcessor::ISQPEventPtr> events;
//            if (coordinatorConfiguration->enableProactiveDeployment) {
            if (false) {
                NES_DEBUG("Inserting predictions")
                events = createEvents(reqJson["predictions"]);
            } else {
                NES_DEBUG("Inserting events")
                events = createEvents(bufferJson);
            }
            // auto events = createEvents(reqJson);
            NES_DEBUG("Processing {} events", events.size());
            if (!events.empty()) {
                requestHandlerService->queueISQPRequest(events, false, reconnectCounter);
            }
            NES_DEBUG("Inserted request messges666666666666666666666666666666666666666666666666666666")
            //bool success = std::static_pointer_cast<RequestProcessor::ISQPRequestResponse>(requestHandlerService->queueISQPRequest(events))->success;
            //            if (success) {
            //                NES_DEBUG("TopologyController::handlePost:addParent: updated topology successfully");
            //            } else {
            //                NES_ERROR("TopologyController::handlePost:addParent: Failed");
            //                return errorHandler->handleError(Status::CODE_500, "TopologyController::handlePost:removeAsParent: Failed");
            //            }
            //Prepare the response
            if (bufferJson.size() != 0) {
                NES_DEBUG("waiting for async calls")
                std::vector<RpcAsyncRequest> asyncRequests;
                asyncRequests.emplace_back(RpcAsyncRequest{completionQueue, RpcClientMode::Unregister});
                workerRPCClient->checkAsyncResult(asyncRequests);
            }
            NES_DEBUG("received results messges666666666666666666666666666666666666666666666666666666")

            nlohmann::json response;
            //            response["success"] = success;
            response["success"] = true;
            return createResponse(Status::CODE_200, response.dump());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server Error");
        }
    }

    ENDPOINT("DELETE", "/removeAsChild", removeParent, BODY_STRING(String, request)) {
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
            WorkerId parentId = reqJson["parentId"].get<WorkerId>();
            WorkerId childId = reqJson["childId"].get<WorkerId>();
            // check if childID is actually a child of parentID
            auto children = topology->getChildTopologyNodeIds(parentId);
            bool contained = false;
            for (const auto& child : children) {
                if (child == childId) {
                    contained = true;
                }
            }
            if (contained) {
                bool removed = topology->removeTopologyNodeAsChild(parentId, childId);
                if (removed) {
                    NES_DEBUG("TopologyController::handlePost:removeParent: deleted link successfully");
                } else {
                    NES_ERROR("TopologyController::handlePost:removeParent: Failed");
                    return errorHandler->handleError(Status::CODE_500, "TopologyController::handlePost:removeAsParent: Failed");
                }
                //Prepare the response
                nlohmann::json response;
                response["success"] = removed;
                return createResponse(Status::CODE_200, response.dump());
            } else {
                return errorHandler->handleError(Status::CODE_400,
                                                 fmt::format("Could not remove parent for node in topology: Node with "
                                                             "childId={} is not a child of node with parentID={}.",
                                                             childId,
                                                             parentId));
            }
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server Error");
        }
    }

  private:
    // create a vector of isqp events from a json array
    std::vector<RequestProcessor::ISQPEventPtr> createEvents(const nlohmann::json& reqJson) {
        std::vector<RequestProcessor::ISQPEventPtr> events;
        for (const auto& event : reqJson) {
            events.push_back(createEvent(event));
        }
        return events;
    }

    //start buffering at all child nodes from the json
    void startBufferingOnAllSources(const nlohmann::json& reqJson, const CompletionQueuePtr& completionQueue) {

        NES_DEBUG("Buffer on moving devices")
        // std::set<uint64_t> buffer_only;
        std::set<WorkerId> moving;
        for (const auto& worker : reqJson) {
            std::string action = worker["action"].get<std::string>();
            if (action == "add" || action == "buffer") {
                WorkerId childId (worker["childId"].get<uint64_t>());
                WorkerId parentId(worker["parentId"].get<uint64_t>());
                NES_DEBUG("Buffering on child {} until connected to parent {}", childId, parentId);
                auto node = topology->getCopyOfTopologyNodeWithId(childId);
                NES_DEBUG("retrieved topology node, getting address");
                //get the adress of the node
                auto ipAddress = node->getIpAddress();
                NES_DEBUG("got address, getting grpc port");
                //get the grpc port
                auto grpcPort = node->getGrpcPort();
                //construct the adress
                std::string address = ipAddress + ":" + std::to_string(grpcPort);
                NES_DEBUG("send buffering request to {}", address);
                workerRPCClient->startBufferingAsync(address, completionQueue, parentId, reconnectCounter);
                NES_DEBUG("sent bufering request")

                moving.insert(childId);
                //buffer_only.insert(sourceNodeMaps[childId].begin(), sourceNodeMaps[parentId].end());
            }
        }

        auto mockParent = INVALID_WORKER_NODE_ID;
        //if ((requestHandlerService->isIncrementalPlacementEnabled() && !coordinatorConfiguration->enableProactiveDeployment) || moving.empty()) {
        if (requestHandlerService->isIncrementalPlacementEnabled() || moving.empty()) {
            NES_DEBUG("incremental placement is enabled, no futher buffering needed")
            // mockParent = 0;
            return;
        }

        if (!sourceNodeMapInitialized) {
            NES_DEBUG("initializing source node map")
            for (const auto& [logicalSourceName, _] : sourceCatalog->getAllLogicalSource()) {
                auto sourceNodes = sourceCatalog->getSourceNodesForLogicalSource(logicalSourceName);
                for (const auto& sourceNode : sourceNodes) {
                    auto& vec = sourceNodeMap[sourceNode];
                    vec.insert(vec.begin(), sourceNodes.cbegin(), sourceNodes.cend());
                }
            }
            //            auto nodeIds = topology->getAllRegisteredNodeIds();
            //            for (const auto& nodeId : nodeIds) {
            //                auto node = topology->lockTopologyNode(nodeId);
            //                if (node->operator*()->getSpatialNodeType() == NES::Spatial::Experimental::SpatialType::MOBILE_NODE) {
            //                    NES_ERROR("adding node {} to mobile nodes", nodeId);
            //                    mobileNodes.push_back(nodeId);
            //                }
            //            }
            sourceNodeMapInitialized = true;
        }

        std::set<WorkerId> buffering;
        NES_DEBUG("send buffering requests to non moving workers")
//        for (const auto& worker : moving) {
//            for (const auto& sourceNode : sourceNodeMap[worker]) {
//
//                if (moving.contains(sourceNode) || buffering.contains(sourceNode)) {
//                    NES_DEBUG("worker {} is moving or buffering, do not send another buffering request", sourceNode);
//                    continue;
//                }
//                NES_DEBUG("worker {} is not moving, send buffering request", sourceNode);
//                //auto node = topology->lockTopologyNode(sourceNode);
//                auto node = topology->getCopyOfTopologyNodeWithId(sourceNode);
//                //get the adress of the node
//                auto ipAddress = node->getIpAddress();
//                //get the grpc port
//                auto grpcPort = node->getGrpcPort();
//                //construct the adress
//                std::string address = ipAddress + ":" + std::to_string(grpcPort);
//                workerRPCClient->startBufferingAsync(address, completionQueue, mockParent);
//                buffering.insert(sourceNode);
//            }
//        }
    }

    //create an add or remove event
    RequestProcessor::ISQPEventPtr createEvent(const nlohmann::json& reqJson) {
        WorkerId parentId(reqJson["parentId"].get<uint64_t>());
        WorkerId childId(reqJson["childId"].get<uint64_t>());
        std::string action = reqJson["action"].get<std::string>();
        if (action == "add") {
            return RequestProcessor::ISQPAddLinkEvent::create(parentId, childId);
        }
        if (action == "remove") {
            return RequestProcessor::ISQPRemoveLinkEvent::create(parentId, childId);
        }
        throw std::logic_error("Invalid action type");
    }

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
        WorkerId parentId = reqJson["parentId"].get<WorkerId>();
        WorkerId childId = reqJson["childId"].get<WorkerId>();
        if (parentId == childId) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add/remove parent for node in topology: childId and parentId must be different.");
        }

        if (!topology->nodeWithWorkerIdExists(childId)) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add/remove parent for node in topology: Node with childId=" + childId.toString() + " not found.");
        }

        if (!topology->nodeWithWorkerIdExists(parentId)) {
            return errorHandler->handleError(
                Status::CODE_400,
                "Could not add/remove parent for node in topology: Node with parentId=" + parentId.toString() + " not found.");
        }
        return std::nullopt;
    }

    TopologyPtr topology;
    ErrorHandlerPtr errorHandler;
    RequestHandlerServicePtr requestHandlerService;
    WorkerRPCClientPtr workerRPCClient;
    CoordinatorConfigurationPtr coordinatorConfiguration;
    std::unordered_map<WorkerId, std::vector<WorkerId>> sourceNodeMap;
    bool sourceNodeMapInitialized = false;
    SourceCatalogPtr sourceCatalog;
    std::vector<uint64_t> mobileNodes;
    std::atomic_uint64_t reconnectCounter;
};
}// namespace REST::Controller
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)
#endif// NES_COORDINATOR_INCLUDE_REST_CONTROLLER_TOPOLOGYCONTROLLER_HPP_
