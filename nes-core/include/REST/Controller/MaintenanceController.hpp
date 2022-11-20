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
#ifndef NES_CORE_INCLUDE_REST_CONTROLLER_MAINTENANCECONTROLLER_HPP_
#define NES_CORE_INCLUDE_REST_CONTROLLER_MAINTENANCECONTROLLER_HPP_

#include <Phases/MigrationType.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Services/MaintenanceService.hpp>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
namespace REST {
namespace Controller {

class MaintenanceController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     */
    MaintenanceController(const std::shared_ptr<ObjectMapper>& objectMapper,
                          Experimental::MaintenanceServicePtr maintenanceService,
                          oatpp::String completeRouterPrefix,
                          ErrorHandlerPtr errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), maintenanceService(maintenanceService),
          errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @return
     */
    static std::shared_ptr<MaintenanceController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                         Experimental::MaintenanceServicePtr maintenanceService,
                                                         ErrorHandlerPtr errorHandler,
                                                         std::string routerPrefixAddition) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<MaintenanceController>(objectMapper, maintenanceService, completeRouterPrefix, errorHandler);
    }

    ENDPOINT("POST", "/scheduleMaintenance", scheduleNodeForMaintenance, BODY_STRING(String, request)) {
        //nlohmann::json library has trouble parsing Oatpp String type
        //we extract a std::string from the Oatpp String type to then be parsed
        std::string req = request.getValue("{}");
        nlohmann::json requestJson = nlohmann::json::parse(req);
        if (!requestJson.contains("id")) {
            NES_ERROR("MaintenanceController: Unable to find Field: id ");
            return errorHandler->handleError(Status::CODE_400, "Field 'id' must be provided");
        }
        if (!requestJson.contains("migrationType")) {
            NES_ERROR("MaintenanceController: Unable to find Field: migrationType ");
            return errorHandler->handleError(Status::CODE_400, "Field 'migrationType' must be provided");
        }
        uint64_t id = requestJson["id"];
        Experimental::MigrationType::Value migrationType =
            Experimental::MigrationType::getFromString(requestJson["migrationType"]);
        auto info = maintenanceService->submitMaintenanceRequest(id, migrationType);
        if (info.first == true) {
            nlohmann::json result{};
            result["Info"] = "Successfully submitted Maintenance Request";
            result["Node Id"] = id;
            result["Migration Type"] = Experimental::MigrationType::toString(migrationType);
            return createResponse(Status::CODE_200, result.dump());
        } else {
            std::string errorMessage = info.second;
            return errorHandler->handleError(Status::CODE_404, errorMessage);
        }
    }

  private:
    Experimental::MaintenanceServicePtr maintenanceService;
    ErrorHandlerPtr errorHandler;
};
}//namespace Controller
}// namespace REST
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)

#endif// NES_CORE_INCLUDE_REST_CONTROLLER_MAINTENANCECONTROLLER_HPP_
