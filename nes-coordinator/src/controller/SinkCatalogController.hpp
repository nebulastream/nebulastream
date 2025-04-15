#ifndef SinkCatalogController_hpp
#define SinkCatalogController_hpp

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"

#include <NebuLi.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController) ///< Begin Codegen

/**
 * Sample Api Controller.
 */
class SinkCatalogController : public oatpp::web::server::api::ApiController {
public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    SinkCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper, const std::shared_ptr<std::unordered_map<std::string, NES::CLI::Sink>> sinkCatalog)
        : oatpp::web::server::api::ApiController(objectMapper), sinkCatalog(sinkCatalog) {}
public:

    ENDPOINT("GET", "/sinks", root)
    {
        nlohmann::json sinks = nlohmann::json::array();
        for (auto const& [key, val] : *sinkCatalog)
        {
            nlohmann::json entry;
            entry["sink_name"] = key;
            sinks.push_back(entry);
        }
        nlohmann::json response_json;
        response_json["sinks"] = sinks;
        auto body = response_json.dump(); // Convert json to string
        auto response = createResponse(Status::CODE_200, oatpp::String(body));
        response->putHeader("Content-Type", "application/json");
        return response;
    }

private:
    std::shared_ptr<std::unordered_map<std::string, NES::CLI::Sink>> sinkCatalog;

};

#include OATPP_CODEGEN_END(ApiController) ///< End Codegen

#endif /* SinkCatalogController_hpp */
