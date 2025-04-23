#pragma once

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"

#include <nlohmann/json.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController) ///< Begin Codegen

/**
 * Sample Api Controller.
 */
class ConnectivityController : public oatpp::web::server::api::ApiController {
public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    ConnectivityController(const std::shared_ptr<ObjectMapper>& objectMapper)
        : oatpp::web::server::api::ApiController(objectMapper) {}
public:

    ENDPOINT("GET", "/healthCheck", healthCheck) {
        nlohmann::json response_json;
        response_json["status"] = "OK";
        auto body = response_json.dump(); // Convert json to string
        auto response = createResponse(Status::CODE_200, oatpp::String(body));
        response->putHeader("Content-Type", "application/json");
        response->putHeader("Access-Control-Allow-Origin", "*"); // Current fix to use swagger
        return response;
    }

};

#include OATPP_CODEGEN_END(ApiController) ///< End Codegen
