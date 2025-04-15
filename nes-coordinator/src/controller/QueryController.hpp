#ifndef QueryController_hpp
#define QueryController_hpp

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"

#include <nlohmann/json.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController) ///< Begin Codegen

/**
 * Sample Api Controller.
 */
class QueryController : public oatpp::web::server::api::ApiController {
public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    QueryController(const std::shared_ptr<ObjectMapper>& objectMapper, const std::shared_ptr<std::map<size_t, std::string>> queryCatalog)
        : oatpp::web::server::api::ApiController(objectMapper), queryCatalog(queryCatalog) {}
public:

    ENDPOINT("GET", "/queries", queryOverview) {
        nlohmann::json queries = nlohmann::json::array();
        for (auto const& [key, val] : *queryCatalog) {
            nlohmann::json entry;
            entry["query_id"] = key;
            entry["status"] = "UNKNOWN";
            entry["code"] = val;
            queries.push_back(entry);
        }
        nlohmann::json response_json;
        response_json["queries"] = queries;
        auto body = response_json.dump(); // Convert json to string
        auto response = createResponse(Status::CODE_200, oatpp::String(body));
        response->putHeader("Content-Type", "application/json");
        return response;
    }

    ENDPOINT("POST", "/queries", querySubmit, BODY_STRING(String, request)) {
        nlohmann::json data = nlohmann::json::parse(request->c_str());
        auto id = queryCatalog->size();
        (*queryCatalog)[id] = data["code"].dump();
        return createResponse(Status::CODE_200, "received");
    }

private:
    std::shared_ptr<std::map<size_t, std::string>> queryCatalog;

};

#include OATPP_CODEGEN_END(ApiController) ///< End Codegen

#endif /* QueryController_hpp */
