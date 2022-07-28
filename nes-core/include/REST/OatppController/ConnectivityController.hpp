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
#ifndef NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_CONNECTIVITYCONTROLLER_HPP_
#define NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_CONNECTIVITYCONTROLLER_HPP_

#include <REST/DTOs/ConnectivityResponse.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
namespace REST {
class ConnectivityController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    ConnectivityController(const std::shared_ptr<ObjectMapper>& objectMapper)
        : oatpp::web::server::api::ApiController(objectMapper) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<ConnectivityController> createShared(const std::shared_ptr<ObjectMapper>& objectMapper) {
        return std::make_shared<ConnectivityController>(objectMapper);
    }

    ENDPOINT("GET", "/check", root) {
        auto dto = ConnectivityResponse::createShared();
        dto->statusCode = 200;
        dto->success = true;
        return createDtoResponse(Status::CODE_200, dto);
    }
};
}// namespace REST
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)

#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_CONNECTIVITYCONTROLLER_HPP_
