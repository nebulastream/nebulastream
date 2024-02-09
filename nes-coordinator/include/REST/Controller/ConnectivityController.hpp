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
#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_CONNECTIVITYCONTROLLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_CONNECTIVITYCONTROLLER_HPP_

#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES::REST::Controller {
class ConnectivityController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     */
    ConnectivityController(const std::shared_ptr<ObjectMapper>& objectMapper, const oatpp::String& completeRouterPrefix)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @return
     */
    static std::shared_ptr<ConnectivityController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                          const std::string& routerPrefixAddition) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<ConnectivityController>(objectMapper, completeRouterPrefix);
    }

    ENDPOINT("GET", "/check", root) {
        nlohmann::json response;
        response["statusCode"] = 200;
        response["success"] = true;
        return createResponse(Status::CODE_200, response.dump());
    }
};
}// namespace NES::REST::Controller

#include OATPP_CODEGEN_END(ApiController)

#endif // NES_COORDINATOR_INCLUDE_REST_CONTROLLER_CONNECTIVITYCONTROLLER_HPP_
