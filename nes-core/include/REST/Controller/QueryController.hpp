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
#ifndef NES_CORE_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_
#define NES_CORE_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_

#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Services/QueryService.hpp>
#include <exception>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST {
namespace Controller {
class QueryController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    QueryController(const std::shared_ptr<ObjectMapper>& objectMapper,
                    QueryServicePtr queryService,
                    QueryCatalogServicePtr queryCatalogService,
                    GlobalExecutionPlanPtr globalExecutionPlan,
                    const oatpp::String& completeRouterPrefix,
                    ErrorHandlerPtr errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), queryService(std::move(queryService)),
          queryCatalogService(std::move(queryCatalogService)), globalExecutionPlan(std::move(globalExecutionPlan)),
          errorHandler(std::move(errorHandler)) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<QueryController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                   QueryServicePtr queryService,
                                                   QueryCatalogServicePtr queryCatalogService,
                                                   GlobalExecutionPlanPtr globalExecutionPlan,
                                                   std::string routerPrefixAddition,
                                                   ErrorHandlerPtr errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<QueryController>(objectMapper,
                                                 queryService,
                                                 queryCatalogService,
                                                 globalExecutionPlan,
                                                 completeRouterPrefix,
                                                 errorHandler);
    }

    ENDPOINT("GET", "/execution-plan", getExecutionPlan, QUERY(UInt64, queryId, "queryId")) {
        try {
            const Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            auto executionPlanJson = PlanJsonGenerator::getExecutionPlanAsJson(globalExecutionPlan, queryId);
            NES_DEBUG("QueryController:: execution-plan: {}", executionPlanJson.dump());
            return createResponse(Status::CODE_200, executionPlanJson.dump());
        } catch (Exceptions::QueryNotFoundException e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/query-plan", getQueryPlan, QUERY(UInt64, queryId, "queryId")) {
        try {
            const Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            NES_TRACE("UtilityFunctions: Getting the json representation of the query plan");
            auto basePlan = PlanJsonGenerator::getQueryPlanAsJson(queryCatalogEntry->getInputQueryPlan());
            return createResponse(Status::CODE_200, basePlan.dump());
        } catch (Exceptions::QueryNotFoundException e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/optimization-phase", getOptimizationPhase, QUERY(UInt64, queryId, "queryId")) {
        try {
            const Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");
            auto optimizationPhases = queryCatalogEntry->getOptimizationPhases();
            nlohmann::json response;
            for (auto const& [phaseName, queryPlan] : optimizationPhases) {
                auto queryPlanJson = PlanJsonGenerator::getQueryPlanAsJson(queryPlan);
                response[phaseName] = queryPlanJson;
            }
            return createResponse(Status::CODE_200, response.dump());
        } catch (Exceptions::QueryNotFoundException e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/query-status", getQueryStatus, QUERY(UInt64, queryId, "queryId")) {
        //NOTE: QueryController has "query-status" endpoint. QueryCatalogController has "status" endpoint with same functionality.
        //Functionality has been duplicated for compatibility.
        try {
            NES_DEBUG("Get current status of the query");
            const Catalogs::Query::QueryCatalogEntryPtr catalogEntry = queryCatalogService->getEntryForQuery(queryId);
            nlohmann::json response;
            response["queryId"] = queryId.getValue(0);
            response["queryString"] = catalogEntry->getQueryString();
            response["status"] = catalogEntry->getQueryStatusAsString();
            response["queryPlan"] = catalogEntry->getInputQueryPlan()->toString();
            response["queryMetaData"] = catalogEntry->getMetaInformation();
            return createResponse(Status::CODE_200, response.dump());
        } catch (Exceptions::QueryNotFoundException e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("POST", "/execute-query", submitQuery, BODY_STRING(String, request)) {
        try {
            //nlohmann::json library has trouble parsing Oatpp String type
            //we extract a std::string from the Oatpp String type to then be parsed
            std::string req = request.getValue("{}");
            nlohmann::json requestJson = nlohmann::json::parse(req);
            auto error = validateUserRequest(requestJson);
            if (error.has_value()) {
                return error.value();
            }
            if (!validatePlacementStrategy(requestJson["placement"].get<std::string>())) {
                std::string errorMessage = "Invalid Placement Strategy: " + requestJson["placement"].get<std::string>()
                    + ". Further info can be found at https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html";
                return errorHandler->handleError(Status::CODE_400, errorMessage);
            }
            auto userQuery = requestJson["userQuery"].get<std::string>();

            std::string placementStrategyString = DEFAULT_PLACEMENT_STRATEGY_TYPE;
            std::string faultToleranceString = DEFAULT_TOLERANCE_TYPE;
            std::string lineageString = DEFAULT_LINEAGE_TYPE;
            if (requestJson.contains("faultTolerance")) {
                if (!validateFaultToleranceType(requestJson["faultTolerance"].get<std::string>())) {
                    std::string errorMessage =
                        "Invalid fault tolerance Type provided: " + requestJson["faultTolerance"].get<std::string>()
                        + ". Valid Fault Tolerance Types are: 'AT_MOST_ONCE', 'AT_LEAST_ONCE', 'EXACTLY_ONCE', 'NONE'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);
                } else {
                    faultToleranceString = requestJson["faultTolerance"].get<std::string>();
                }
            }
            if (requestJson.contains("lineage")) {
                if (!validateLineageMode(requestJson["lineage"].get<std::string>())) {
                    NES_ERROR("QueryController: handlePost -execute-query: Invalid Lineage Type provided: {}", lineageString);
                    std::string errorMessage = "Invalid Lineage Mode Type provided: " + lineageString
                        + ". Valid Lineage Modes are: 'IN_MEMORY', 'PERSISTENT', 'REMOTE', 'NONE'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);
                } else {
                    lineageString = requestJson["lineage"].get<std::string>();
                }
            }
            if (requestJson.contains("placement")) {
                if (!validatePlacementStrategy(placementStrategyString = requestJson["placement"].get<std::string>())) {
                    NES_ERROR("QueryController: handlePost -execute-query: Invalid Placement Strategy Type provided: {}",
                              placementStrategyString);
                    std::string errorMessage = "Invalid Placement Strategy Type provided: " + placementStrategyString
                        + ". Valid Placement Strategies are: 'IN_MEMORY', 'PERSISTENT', 'REMOTE', 'NONE'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);
                } else {
                    placementStrategyString = requestJson["placement"].get<std::string>();
                }
            }

            auto faultToleranceMode = magic_enum::enum_cast<FaultToleranceType>(faultToleranceString).value();
            auto lineageMode = magic_enum::enum_cast<LineageType>(lineageString).value();
            auto placement = magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategyString).value();
            NES_DEBUG("QueryController: handlePost -execute-query: Params: userQuery= {}, strategyName= {}, faultTolerance= {}, "
                      "lineage= {}",
                      userQuery,
                      placementStrategyString,
                      faultToleranceString,
                      lineageString);
            QueryId queryId =
                queryService->validateAndQueueAddQueryRequest(userQuery, placement, faultToleranceMode, lineageMode);
            //Prepare the response
            nlohmann::json response;
            response["queryId"] = queryId;
            return createResponse(Status::CODE_202, response.dump());
        } catch (const InvalidQueryException& exc) {
            NES_ERROR("QueryController: handlePost -execute-query: Exception occurred during submission of a query "
                      "user request: {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_400, exc.what());
        } catch (const MapEntryNotFoundException& exc) {
            NES_ERROR("QueryController: handlePost -execute-query: Exception occurred during submission of a query "
                      "user request: {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_400, exc.what());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server Error");
        }
    }

    ENDPOINT("POST", "/execute-query-ex", submitQueryProtobuf, BODY_STRING(String, request)) {
        try {
            std::shared_ptr<SubmitQueryRequest> protobufMessage = std::make_shared<SubmitQueryRequest>();
            auto optional = validateProtobufMessage(protobufMessage, request);
            if (optional.has_value()) {
                return optional.value();
            }
            SerializableQueryPlan* queryPlanSerialized = protobufMessage->mutable_queryplan();
            QueryPlanPtr queryPlan(QueryPlanSerializationUtil::deserializeQueryPlan(queryPlanSerialized));
            auto* context = protobufMessage->mutable_context();

            std::string placementStrategyString = DEFAULT_PLACEMENT_STRATEGY_TYPE;
            std::string faultToleranceString = DEFAULT_TOLERANCE_TYPE;
            std::string lineageString = DEFAULT_TOLERANCE_TYPE;
            if (context->contains("faultTolerance")) {
                if (!validateFaultToleranceType(context->at("faultTolerance").value())) {
                    std::string errorMessage = "Invalid fault tolerance Type provided: " + context->at("faultTolerance").value()
                        + ". Valid Fault Tolerance Types are: 'AT_MOST_ONCE', 'AT_LEAST_ONCE', 'EXACTLY_ONCE', 'NONE'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);
                } else {
                    faultToleranceString = context->at("faultTolerance").value();
                }
            }
            if (context->contains("lineage")) {
                if (!validateLineageMode(lineageString = context->at("lineage").value())) {
                    NES_ERROR("QueryController: handlePost -execute-query: Invalid Lineage Type provided: {}", lineageString);
                    std::string errorMessage = "Invalid Lineage Mode Type provided: " + lineageString
                        + ". Valid Lineage Modes are: 'IN_MEMORY', 'PERSISTENT', 'REMOTE', 'NONE'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);
                } else {
                    lineageString = context->at("lineage").value();
                }
            }
            if (context->contains("placement")) {
                if (!validatePlacementStrategy(placementStrategyString = context->at("placement").value())) {
                    NES_ERROR("QueryController: handlePost -execute-query: Invalid Placement Strategy Type provided: {}",
                              placementStrategyString);
                    std::string errorMessage = "Invalid Placement Strategy Type provided: " + placementStrategyString
                        + ". Valid Placement Strategies are: 'IN_MEMORY', 'PERSISTENT', 'REMOTE', 'NONE'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);
                } else {
                    placementStrategyString = context->at("placement").value();
                }
            }

            std::string* queryString = protobufMessage->mutable_querystring();
            auto placementStrategy = magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategyString).value();
            auto faultToleranceMode = magic_enum::enum_cast<FaultToleranceType>(faultToleranceString).value();
            auto lineageType = magic_enum::enum_cast<LineageType>(lineageString).value();
            QueryId queryId =
                queryService->addQueryRequest(*queryString, queryPlan, placementStrategy, faultToleranceMode, lineageType);

            //Prepare the response
            nlohmann::json response;
            response["queryId"] = queryId;
            return createResponse(Status::CODE_202, response.dump());
        } catch (nlohmann::json::exception e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (const std::exception& exc) {
            NES_ERROR("QueryController: handlePost -execute-query-ex: Exception occurred while building the query plan for "
                      "user request: {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_400, exc.what());
        } catch (...) {
            NES_ERROR("RestServer: unknown exception.");
            return errorHandler->handleError(Status::CODE_500, "unknown exception");
        }
    }

    ENDPOINT("DELETE", "/stop-query", stopQuery, QUERY(UInt64, queryId, "queryId")) {
        try {
            bool success = queryService->validateAndQueueStopQueryRequest(queryId);
            Status status = success
                ? Status::CODE_202
                : Status::
                    CODE_400;//QueryController catches InvalidQueryStatus exception, but this is never thrown since it was commented out
            nlohmann::json response;
            response["success"] = success;
            return createResponse(status, response.dump());
        } catch (Exceptions::QueryNotFoundException e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (...) {
            NES_ERROR("RestServer: unknown exception.");
            return errorHandler->handleError(Status::CODE_500, "unknown exception");
        }
    }

  private:
    std::optional<std::shared_ptr<oatpp::web::protocol::http::outgoing::Response>>
    validateUserRequest(nlohmann::json userRequest) {
        if (!userRequest.contains("userQuery")) {
            NES_ERROR("QueryController: handlePost -execute-query: Wrong key word for user query, use 'userQuery'.");
            std::string errorMessage = "Incorrect or missing key word for user query, use 'userQuery'. For more info check "
                                       "https://docs.nebula.stream/docs/clients/rest-api/";
            return errorHandler->handleError(Status::CODE_400, errorMessage);
        }
        if (!userRequest.contains("placement")) {
            NES_ERROR("QueryController: handlePost -execute-query: No placement strategy specified. Specify a placement strategy "
                      "using 'placement'.");
            std::string errorMessage = "No placement strategy specified. Specify a placement strategy using 'placement'. For "
                                       "more info check https://docs.nebula.stream/docs/clients/rest-api/";
            return errorHandler->handleError(Status::CODE_400, errorMessage);
        }
        return std::nullopt;
    }

    bool validatePlacementStrategy(const std::string& placementStrategy) {
        return magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategy).has_value();
    }

    bool validateFaultToleranceType(const std::string& faultToleranceString) {
        return magic_enum::enum_cast<FaultToleranceType>(faultToleranceString).has_value();
    }

    std::optional<std::shared_ptr<oatpp::web::protocol::http::outgoing::Response>>
    validateProtobufMessage(const std::shared_ptr<SubmitQueryRequest>& protobufMessage, const std::string& body) {
        if (!protobufMessage->ParseFromArray(body.data(), body.size())) {
            return errorHandler->handleError(Status::CODE_400, "Invalid Protobuf Message");
        }
        auto* context = protobufMessage->mutable_context();
        if (!context->contains("placement")) {
            NES_ERROR("QueryController: handlePost -execute-query: No placement strategy specified. Specify a placement strategy "
                      "using 'placementStrategy'.");
            std::string errorMessage = "No placement strategy specified. Specify a placement strategy using 'placementStrategy'."
                                       "More info at: https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html";
            return errorHandler->handleError(Status::CODE_400, errorMessage);
        }
        std::string placementStrategy = context->at("placement").value();
        if (!validatePlacementStrategy(placementStrategy)) {
            std::string errorMessage = "Invalid Placement Strategy: " + placementStrategy
                + ". Further info can be found at https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html";
            return errorHandler->handleError(Status::CODE_400, errorMessage);
        }
        return std::nullopt;
    }

    bool validateLineageMode(const std::string& lineageModeString) {
        return magic_enum::enum_cast<LineageType>(lineageModeString).has_value();
    }

    const std::string DEFAULT_PLACEMENT_STRATEGY_TYPE = "NONE";
    const std::string DEFAULT_TOLERANCE_TYPE = "NONE";
    const std::string DEFAULT_LINEAGE_TYPE = "NONE";
    QueryServicePtr queryService;
    QueryCatalogServicePtr queryCatalogService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    ErrorHandlerPtr errorHandler;
};
}//namespace Controller
}// namespace REST
}// namespace NES
#endif// NES_CORE_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_
