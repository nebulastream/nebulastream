#pragma once

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"

#include <Sources/SourceCatalog.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController) ///< Begin Codegen

/**
 * Sample Api Controller.
 */
class SourceCatalogController : public oatpp::web::server::api::ApiController {
public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    SourceCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper, const std::shared_ptr<NES::SourceCatalog> sourceCatalog)
        : oatpp::web::server::api::ApiController(objectMapper), sourceCatalog(sourceCatalog) {}
public:

    ENDPOINT("GET", "/sources", root) {
        auto logicalSources = sourceCatalog->getAllLogicalSources();

        nlohmann::json logicalSourcesJson = nlohmann::json::array();;
        for (auto const& val : logicalSources) {
            logicalSourcesJson.push_back(val.getLogicalSourceName());
        }

        auto body = logicalSourcesJson.dump(); // Convert json to string
        auto response = createResponse(Status::CODE_200, oatpp::String(body));
        response->putHeader("Content-Type", "application/json");
        response->putHeader("Access-Control-Allow-Origin", "*"); // Current fix to use swagger
        return response;
    }

private:
    std::shared_ptr<NES::SourceCatalog> sourceCatalog;

};

#include OATPP_CODEGEN_END(ApiController) ///< End Codegen
