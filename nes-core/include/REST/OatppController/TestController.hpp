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

/***************************************************************************
 *
 * Project         _____    __   ____   _      _
 *                (  _  )  /__\ (_  _)_| |_  _| |_
 *                 )(_)(  /(__)\  )( (_   _)(_   _)
 *                (_____)(__)(__)(__)  |_|    |_|
 *
 *
 * Copyright 2018-present, Leonid Stryzhevskyi <lganzzzo@gmail.com>
 *
 ***************************************************************************/

#ifndef NES_NES_CORE_INCLUDE_REST_CONTROLLER_TESTCONTROLLER_HPP_
#define NES_NES_CORE_INCLUDE_REST_CONTROLLER_TESTCONTROLLER_HPP_

#include <REST/DTOs/ConnectivityResponse.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController) ///< Begin Codegen

/**
 * Sample Api Controller.
 */
class TestController : public oatpp::web::server::api::ApiController {
  public:
    /**
   * Constructor with object mapper.
   * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
   */
    TestController(OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper))
        : oatpp::web::server::api::ApiController(objectMapper)
    {}
  public:

    ENDPOINT("GET", "/hello", root) {
        auto dto = ConnectivityResponse::createShared();
        dto->statusCode = 200;
        dto->success = true;
        return createDtoResponse(Status::CODE_200, dto);
    }

};

#include OATPP_CODEGEN_END(ApiController) ///< End Codegen

#endif//NES_NES_CORE_INCLUDE_REST_CONTROLLER_TESTCONTROLLER_HPP_
