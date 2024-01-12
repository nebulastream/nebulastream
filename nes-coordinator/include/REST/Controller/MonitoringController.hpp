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

#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_MONITORINGCONTROLLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_MONITORINGCONTROLLER_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/MonitoringService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanJsonGenerator.hpp>
#include <memory>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/core/parser/Caret.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <string>
#include <utility>
#include <vector>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
namespace Runtime {
class BufferManager;
using BufferManagerPtr = std::shared_ptr<BufferManager>;
}// namespace Runtime
class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;
class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST::Controller {
class MonitoringController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param monitoringService
     * @param BufferManager
     * @param ErrorHandler - for sending error messages via DTO
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "monitoringController")
     */
    MonitoringController(const std::shared_ptr<ObjectMapper>& objectMapper,
                         const MonitoringServicePtr& mService,
                         const Runtime::BufferManagerPtr& bManager,
                         const ErrorHandlerPtr& eHandler,
                         const oatpp::String& completeRouterPrefix)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), monitoringService(mService),
          bufferManager(bManager), errorHandler(eHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param MonitoringServicePtr
     * @param BufferManagerPtr
     * @param ErrorHandlerPtr - for sending error messages via DTO
     * @param routerPrefixAddition - controller specific router prefix (e.g "monitoringController/")
     * @return MonitoringController
     */
    static std::shared_ptr<MonitoringController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                        const MonitoringServicePtr& mService,
                                                        const Runtime::BufferManagerPtr& bManager,
                                                        const ErrorHandlerPtr& errorHandler,
                                                        const std::string& routerPrefixAddition) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<MonitoringController>(objectMapper, mService, bManager, errorHandler, completeRouterPrefix);
    }

    ENDPOINT("GET", "/start", getMonitoringControllerStart) {
        if (!monitoringService->isMonitoringEnabled()) {
            return errorHandler->handleError(Status::CODE_500, "Error: Monitoring ist not enabled.");
        }
        nlohmann::json responseMsg;
        responseMsg = monitoringService->startMonitoringStreams();
        if (responseMsg == nullptr) {
            return errorHandler->handleError(Status::CODE_500, "Request was not successful.");
        }
        return createResponse(Status::CODE_200, responseMsg.dump());
    }

    ENDPOINT("GET", "/stop", getMonitoringControllerStop) {
        if (!monitoringService->isMonitoringEnabled()) {
            return errorHandler->handleError(Status::CODE_500, "Error: Monitoring ist not enabled.");
        }
        nlohmann::json responseMsg;
        responseMsg = monitoringService->stopMonitoringStreams();
        if (responseMsg == true) {
            return createResponse(Status::CODE_200, responseMsg.dump());
        }
        return errorHandler->handleError(Status::CODE_500, "Stopping monitoring service was not successful.");
    }

    ENDPOINT("GET", "/streams", getMonitoringControllerStreams) {
        if (!monitoringService->isMonitoringEnabled()) {
            return errorHandler->handleError(Status::CODE_500, "Error: Monitoring ist not enabled.");
        }
        nlohmann::json response;
        response = monitoringService->getMonitoringStreams();
        if (response == nullptr) {
            return errorHandler->handleError(Status::CODE_500, "Getting streams of monitoring service was not successful.");
        }
        return createResponse(Status::CODE_200, response.dump());
    }

    ENDPOINT("GET", "/storage", getMonitoringControllerStorage) {
        if (!monitoringService->isMonitoringEnabled()) {
            return errorHandler->handleError(Status::CODE_500, "Error: Monitoring ist not enabled.");
        }
        nlohmann::json response;
        response = monitoringService->requestNewestMonitoringDataFromMetricStoreAsJson();
        if (response == nullptr) {
            return errorHandler->handleError(
                Status::CODE_500,
                "Getting newest monitoring data from metric store of monitoring service was not successful.");
        }
        return createResponse(Status::CODE_200, response.dump());
    }

    ENDPOINT("GET", "/metrics", getMonitoringControllerDataFromAllNodes) {
        if (!monitoringService->isMonitoringEnabled()) {
            return errorHandler->handleError(Status::CODE_500, "Error: Monitoring ist not enabled.");
        }
        nlohmann::json response;
        response = monitoringService->requestMonitoringDataFromAllNodesAsJson();
        if (response == nullptr) {
            return errorHandler->handleError(Status::CODE_500, "Getting monitoring data from all nodes was not successful.");
        }
        return createResponse(Status::CODE_200, response.dump());
    }

    ENDPOINT("GET", "/metrics", getMonitoringControllerDataFromOneNode, QUERY(UInt64, nodeId, "nodeId")) {
        if (!monitoringService->isMonitoringEnabled()) {
            return errorHandler->handleError(Status::CODE_500, "Error: Monitoring ist not enabled.");
        }
        try {
            nlohmann::json response;
            response = monitoringService->requestMonitoringDataAsJson(nodeId);
            if (!(response == nullptr)) {
                return createResponse(Status::CODE_200, response.dump());
            }
        } catch (std::runtime_error& ex) {
            std::string errorMsg = ex.what();
            return errorHandler->handleError(Status::CODE_500, errorMsg);
        }
        return errorHandler->handleError(Status::CODE_500, "Resource not found.");
    }

  private:
    MonitoringServicePtr monitoringService;
    Runtime::BufferManagerPtr bufferManager;
    ErrorHandlerPtr errorHandler;
};
}// namespace REST::Controller
}// namespace NES
#include OATPP_CODEGEN_END(ApiController)
#endif// NES_COORDINATOR_INCLUDE_REST_CONTROLLER_MONITORINGCONTROLLER_HPP_
