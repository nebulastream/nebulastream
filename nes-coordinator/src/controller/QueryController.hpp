#pragma once

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"

#include <google/protobuf/text_format.h>
#include <grpc++/create_channel.h>
#include <nlohmann/json.hpp>
#include <NebuLI.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SourceCatalogs/SourceCatalog.hpp>

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
    QueryController(
        const std::shared_ptr<ObjectMapper>& objectMapper,
        std::shared_ptr<std::map<size_t, std::string>> queryCatalog,
        std::shared_ptr<NES::Catalogs::Source::SourceCatalog> sourceCatalog,
        std::shared_ptr<std::unordered_map<std::string, NES::CLI::Sink>> sinkCatalog)
        : oatpp::web::server::api::ApiController(objectMapper), queryCatalog(queryCatalog), sourceCatalog(sourceCatalog), sinkCatalog(sinkCatalog) {}
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
        response->putHeader("Access-Control-Allow-Origin", "*"); // Current fix to use swagger
        return response;
    }

    ENDPOINT("POST", "/queries", querySubmit, BODY_STRING(String, request)) {
        nlohmann::json data = nlohmann::json::parse(request->c_str());
        auto id = queryCatalog->size();
        auto query_str = data["code"].get<std::string>();
        (*queryCatalog)[id] = query_str;

        nlohmann::json response_json;
        response_json["query_id"] = id;
        response_json["status"] = "UNKNOWN";
        auto body = response_json.dump(); // Convert json to string
        auto response = createResponse(Status::CODE_200, oatpp::String(body));
        response->putHeader("Content-Type", "application/json");
        response->putHeader("Access-Control-Allow-Origin", "*"); // Current fix to use swagger
        return response;
    }

    ENDPOINT("GET", "/queries/{query_id}/plan", getQueryPlan,
             PATH(Int32, query_id, "query_id")) {
        auto id = query_id.getValue(-1);
        auto query_str = (*queryCatalog)[id];

        auto decomposedQueryPlan = createFullySpecifiedQueryPlan2(query_str, sourceCatalog, sinkCatalog);
        std::string output;
        NES::SerializableDecomposedQueryPlan serialized;
        NES::DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(*decomposedQueryPlan, &serialized);
        google::protobuf::TextFormat::PrintToString(serialized, &output);

        nlohmann::json response_json;
        response_json["query_id"] = id;
        response_json["code"] = query_str;
        response_json["plan"] = output;
        auto body = response_json.dump(); // Convert json to string
        auto response = createResponse(Status::CODE_200, oatpp::String(body));
        response->putHeader("Content-Type", "application/json");
        response->putHeader("Access-Control-Allow-Origin", "*"); // Current fix to use swagger
        return response;
    }

private:
    std::shared_ptr<std::map<size_t, std::string>> queryCatalog;
    std::shared_ptr<NES::Catalogs::Source::SourceCatalog> sourceCatalog;
    std::shared_ptr<std::unordered_map<std::string, NES::CLI::Sink>> sinkCatalog;
};

#include OATPP_CODEGEN_END(ApiController) ///< End Codegen
