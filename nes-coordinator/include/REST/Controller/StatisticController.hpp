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

#ifndef NES_NES_COORDINATOR_INCLUDE_REST_CONTROLLER_STATISTICCONTROLLER_HPP_
#define NES_NES_COORDINATOR_INCLUDE_REST_CONTROLLER_STATISTICCONTROLLER_HPP_

#include <REST/Controller/BaseRouterPrefix.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace Experimental::Statistics {

class StatisticController : public oatpp::web::server::api::ApiController {

  public:
    StatisticController(const std::shared_ptr<ObjectMapper>& objectMapper,
                        const NesCoordinatorWeakPtr& coordinator,
                        const oatpp::String& completeRouterPrefix,
                        const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), coordinator(coordinator),
          errorHandler(errorHandler) {}

    static std::shared_ptr<StatisticController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                       const NesCoordinatorWeakPtr& coordinator,
                                                       const std::string& routerPrefixAddition,
                                                       const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<StatisticController>(objectMapper, coordinator, completeRouterPrefix, errorHandler);
    }

  private:
    NesCoordinatorWeakPtr coordinator;
    ErrorHandlerPtr errorHandler;
};
}// namespace Experimental::Statistics
}// namespace NES

#endif//NES_NES_COORDINATOR_INCLUDE_REST_CONTROLLER_STATISTICCONTROLLER_HPP_
