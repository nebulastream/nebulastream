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

    ENDPOINT("GET", "/hello", router) {
        auto dto = ConnectivityResponse::createShared();
        dto->statusCode = 200;
        dto->message = "Hello World!";
        return createDtoResponse(Status::CODE_200, dto);
    }

};

#include OATPP_CODEGEN_END(ApiController) ///< End Codegen

#endif//NES_NES_CORE_INCLUDE_REST_CONTROLLER_TESTCONTROLLER_HPP_
