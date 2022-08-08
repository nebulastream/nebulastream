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

#ifndef NEBULASTREAM_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_MONITORINGCONTROLLER_HPP_
#define NEBULASTREAM_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_MONITORINGCONTROLLER_HPP_

#include <REST/DTOs/MonitoringControllerStringResponse.hpp>
#include <REST/DTOs/MonitoringControllerBoolResponse.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/OatppController/BaseRouterPrefix.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Services/MonitoringService.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>
#include <string>
#include <vector>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <cpprest/json.h>
#include <REST/Handlers/ErrorHandler.hpp>
#include <oatpp/core/parser/Caret.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
namespace Runtime{
class BufferManager;
using BufferManagerPtr = std::shared_ptr<BufferManager>;
}
class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;
class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST {
namespace Controller {
class MonitoringController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param monitoringService
     * @param BufferManager
     * @param ErrorHandler
     * @param routerprefix - part of the path of the request
     */
    MonitoringController(const std::shared_ptr<ObjectMapper>& objectMapper, MonitoringServicePtr mService, Runtime::BufferManagerPtr bManager, ErrorHandlerPtr eHandler, oatpp::String completeRouterPrefix)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), monitoringService(mService), bufferManager(bManager), errorHandler(eHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @param MonitoringServicePtr, BufferManagerPtr, ErrorHandlerPtr, routerPrefixAddition
     * @return MonitoringController
     */
    static std::shared_ptr<MonitoringController> createShared(const std::shared_ptr<ObjectMapper>& objectMapper, MonitoringServicePtr mService, Runtime::BufferManagerPtr bManager, ErrorHandlerPtr errorHandler, std::string routerPrefixAddition) {
        oatpp::String completeRouterPrefix = baseRouterPrefix + routerPrefixAddition;
        return std::make_shared<MonitoringController>(objectMapper, mService, bManager, errorHandler, completeRouterPrefix);
    }

    ENDPOINT("GET", "/start", getMonitoringControllerStart) {
        auto dto = MonitoringControllerStringResponse::createShared();
        dto->monitoringData = monitoringService->startMonitoringStreams().to_string();
        if (dto->monitoringData != "null"){
            return createDtoResponse(Status::CODE_200, dto);
        }
        return errorHandler->handleError(Status::CODE_404, "Starting monitoring service was not successful.");
    }

    ENDPOINT("GET", "/stop", getMonitoringControllerStop) {
        auto dto = MonitoringControllerBoolResponse::createShared();
        dto->success = monitoringService->stopMonitoringStreams().as_bool();
        if (dto->success == true){
            return createDtoResponse(Status::CODE_200, dto);
        }
        return errorHandler->handleError(Status::CODE_404, "Stopping monitoring service was not successful.");
    }

    ENDPOINT("GET", "/streams", getMonitoringControllerStreams) {
        auto dto = MonitoringControllerStringResponse::createShared();
        dto->monitoringData = monitoringService->getMonitoringStreams().to_string();
        if (dto->monitoringData != "null"){
            return createDtoResponse(Status::CODE_200, dto);
        }
        return errorHandler->handleError(Status::CODE_404, "Getting streams of monitoring service was not successful.");
    }

    ENDPOINT("GET", "/storage", getMonitoringControllerStorage) {
        auto dto = MonitoringControllerStringResponse::createShared();
        dto->monitoringData = monitoringService->requestNewestMonitoringDataFromMetricStoreAsJson().to_string();
        if (dto->monitoringData != "null"){
            return createDtoResponse(Status::CODE_200, dto);
        }
        return errorHandler->handleError(Status::CODE_404, "Getting newest monitoring data from metric store of monitoring service was not successful.");
    }

    ENDPOINT("GET", "/Monitoring/metrics", getMonitoringControllerDataFromAllNodes) {
        auto dto = MonitoringControllerStringResponse::createShared();
        dto->monitoringData = monitoringService->requestMonitoringDataFromAllNodesAsJson().to_string();
        if (dto->monitoringData != "null"){
            return createDtoResponse(Status::CODE_200, dto);
        }
        return errorHandler->handleError(Status::CODE_404, "Getting monitoring data from all nodes was not successful.");
    }

    ENDPOINT("GET", "/Monitoring/metrics/", getMonitoringControllerDataFromOneNode,
             QUERY(UInt64, nodeId, "queryId")) {
        auto dto = MonitoringControllerStringResponse::createShared();
        try {
            dto->monitoringData = monitoringService->requestMonitoringDataAsJson(nodeId).to_string();
            return createDtoResponse(Status::CODE_200, dto);
        } catch (std::runtime_error& ex) {
            std::string errorMsg = ex.what();
            return errorHandler->handleError(Status::CODE_400, errorMsg);
        }
        return errorHandler->handleError(Status::CODE_404, "Resource not found.");
    }

  private:
    MonitoringServicePtr monitoringService;
    Runtime::BufferManagerPtr bufferManager;
    ErrorHandlerPtr errorHandler;
};
}// namespace Controller
}// namespace REST
}// namespace NES
#include OATPP_CODEGEN_END(ApiController)
#endif//NEBULASTREAM_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_MONITORINGCONTROLLER_HPP_
