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
#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_LOCATIONCONTROLLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_LOCATIONCONTROLLER_HPP_

#include <Catalogs/Topology/Topology.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/DTOs/ErrorResponse.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>

#include OATPP_CODEGEN_BEGIN(ApiController)
namespace NES::REST::Controller {
class LocationController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     */
    LocationController(const std::shared_ptr<ObjectMapper>& objectMapper,
                       const oatpp::String& completeRouterPrefix,
                       const TopologyPtr& topology,
                       const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), topology(topology),
          errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @return
     */
    static std::shared_ptr<LocationController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                      const TopologyPtr& topology,
                                                      const std::string& routerPrefixAddition,
                                                      const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<LocationController>(objectMapper, completeRouterPrefix, topology, errorHandler);
    }

    ENDPOINT("GET", "", getLocationInformationOfASingleNode, QUERY(UInt64, nodeId, "nodeId")) {
        auto nodeLocationJson = topology->requestNodeLocationDataAsJson(WorkerId(nodeId));
        if (nodeLocationJson == nullptr) {
            NES_ERROR("node with id {} does not exist", nodeId);
            return errorHandler->handleError(Status::CODE_404, "No node with Id: " + std::to_string(nodeId));
        }
        return createResponse(Status::CODE_200, nodeLocationJson.dump());
    }

    ENDPOINT("GET", "/allMobile", getLocationDataOfAllMobileNodes) {
        auto locationsJson = topology->requestLocationAndParentDataFromAllMobileNodes();
        return createResponse(Status::CODE_200, locationsJson.dump());
    }

    ENDPOINT("GET", "/reconnectSchedule", getReconnectionScheduleOfASingleNode, QUERY(UInt64, nodeId, "nodeId")) {
        NES_INFO("Received request to fetch reconnect schedule for node with id {}", nodeId);
        return errorHandler->handleError(Status::CODE_404, "Endpoint Not Implemented");
    }

  private:
    TopologyPtr topology;
    ErrorHandlerPtr errorHandler;
};
}// namespace NES::REST::Controller
#include OATPP_CODEGEN_END(ApiController)
#endif// NES_COORDINATOR_INCLUDE_REST_CONTROLLER_LOCATIONCONTROLLER_HPP_
