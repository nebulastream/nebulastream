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
#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_

#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Operators/Serialization/QueryPlanSerializationUtil.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Services/RequestHandlerService.hpp>
#include <exception>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>

#include OATPP_CODEGEN_BEGIN(ApiController)

#include <RequestProcessor/RequestTypes/ExplainRequest.hpp>
#include <Util/FaultToleranceType.hpp>

namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

class RequestHandlerService;
using RequestHandlerServicePtr = std::shared_ptr<RequestHandlerService>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST::Controller {
class QueryController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    QueryController(const std::shared_ptr<ObjectMapper>& objectMapper,
                    const RequestHandlerServicePtr& requestHandlerService,
                    const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                    const GlobalQueryPlanPtr& globalQueryPlan,
                    const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                    const std::string& completeRouterPrefix,
                    const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix),
          requestHandlerService(requestHandlerService), queryCatalog(queryCatalog), globalQueryPlan(globalQueryPlan),
          globalExecutionPlan(globalExecutionPlan), errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<QueryController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                   const RequestHandlerServicePtr& requestHandlerService,
                                                   const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                                   const GlobalQueryPlanPtr& globalQueryPlan,
                                                   const Optimizer::GlobalExecutionPlanPtr& globalExecutionPlan,
                                                   const std::string& routerPrefixAddition,
                                                   const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<QueryController>(objectMapper,
                                                 requestHandlerService,
                                                 queryCatalog,
                                                 globalQueryPlan,
                                                 globalExecutionPlan,
                                                 completeRouterPrefix,
                                                 errorHandler);
    }

    ENDPOINT("GET", "/execution-plan", getExecutionPlan, QUERY(UInt64, queryId, "queryId")) {
        try {
            //throws an exception if query with the provided id does not exists
            queryCatalog->getQueryEntry(QueryId(queryId));
            //find the shared query plan id hosting the query
            auto sharedQueryPlanId = globalQueryPlan->getSharedQueryId(QueryId(queryId));
            //Return the execution nodes running the shared query plan
            auto executionPlanJson = globalExecutionPlan->getAsJson(sharedQueryPlanId);
            NES_DEBUG("QueryController:: execution-plan: {}", executionPlanJson.dump());
            return createResponse(Status::CODE_200, executionPlanJson.dump());
        } catch (Exceptions::QueryNotFoundException& e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (nlohmann::json::exception& e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/query-plan", getQueryPlan, QUERY(UInt64, queryId, "queryId")) {
        try {
            auto response = queryCatalog->getQueryEntry(QueryId(queryId));
            return createResponse(Status::CODE_200, response.dump());
        } catch (Exceptions::QueryNotFoundException& e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (nlohmann::json::exception& e) {
            return errorHandler->handleError(Status::CODE_500, e.what());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/optimization-phase", getOptimizationPhase, QUERY(UInt64, queryId, "queryId")) {
        try {
            auto response = queryCatalog->getQueryEntry(QueryId(queryId));
            return createResponse(Status::CODE_200, response.dump());
        } catch (Exceptions::QueryNotFoundException& e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        } catch (nlohmann::json::exception& e) {
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
            auto response = queryCatalog->getQueryEntry(QueryId(queryId));
            return createResponse(Status::CODE_200, response.dump());
        } catch (Exceptions::QueryNotFoundException& e) {
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
            std::string faultToleranceString = DEFAULT_TOLERANCE_TYPE;
            if (!validateFaultToleranceType(requestJson["faultTolerance"].get<std::string>())) {
                std::string errorMessage = "Invalid Fault Tolerance Strategy Type provided: " + faultToleranceString
+ ". Valid FaultTolerance Strategies are: 'UB', 'AS', 'CH', 'M'.";
                return errorHandler->handleError(Status::CODE_400, errorMessage);;
            }
            if (!validatePlacementStrategy(requestJson["placement"].get<std::string>())) {
                std::string errorMessage = "Invalid Placement Strategy: " + requestJson["placement"].get<std::string>()
                    + ". Further info can be found at https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html";
                return errorHandler->handleError(Status::CODE_400, errorMessage);
            }
            auto faultToleranceMode = stringToFaultToleranceTypeMap(faultToleranceString);
            auto userQuery = requestJson["userQuery"].get<std::string>();

            std::string placementStrategyString = DEFAULT_PLACEMENT_STRATEGY_TYPE;

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

            auto placement = magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategyString).value();
            NES_DEBUG("QueryController: handlePost -execute-query: Params: userQuery= {}, strategyName= {}",
                      userQuery,
                      placementStrategyString);
            QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(userQuery, placement,
                                                                               faultToleranceMode);
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
        } catch (nlohmann::json::exception& e) {
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
            if (context->contains("faultTolerance")) {
                if (!validateFaultToleranceType(faultToleranceString = context->at("faultTolerance").value())) {
                    std::string errorMessage = "Invalid Fault Tolerance Strategy Type provided: " + faultToleranceString
    + ". Valid FaultTolerance Strategies are: 'UB', 'AS', 'CH', 'M'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);;
                } else {
                    faultToleranceString = context->at("faultTolerance").value();
                }
            }

            auto placementStrategy = magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategyString).value();
            auto faultToleranceMode = stringToFaultToleranceTypeMap(faultToleranceString);

            QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(queryPlan, placementStrategy);

            //Prepare the response
            nlohmann::json response;
            response["queryId"] = queryId;
            return createResponse(Status::CODE_202, response.dump());
        } catch (nlohmann::json::exception& e) {
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

    ENDPOINT("POST", "/explain", explainQuery, BODY_STRING(String, request)) {
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

            auto placementStrategy = magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategyString).value();
            nlohmann::json response = requestHandlerService->validateAndQueueExplainQueryRequest(queryPlan, placementStrategy);

            return createResponse(Status::CODE_202, response.dump());
        } catch (nlohmann::json::exception& e) {
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
            bool success = requestHandlerService->validateAndQueueStopQueryRequest(QueryId(queryId));
            Status status = success ? Status::CODE_202 : Status::CODE_400;//QueryController catches
                // InvalidQueryStatus exception, but this is never thrown since it was commented out
            nlohmann::json response;
            response["success"] = success;
            return createResponse(status, response.dump());
        } catch (Exceptions::QueryNotFoundException& e) {
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

    bool QueryController::validateFaultToleranceType(const std::string& faultToleranceString) {
        auto faultToleranceMode = stringToFaultToleranceTypeMap(faultToleranceString);
        return faultToleranceMode == FaultToleranceType::INVALID;
    }

    bool validatePlacementStrategy(const std::string& placementStrategy) {
        return magic_enum::enum_cast<Optimizer::PlacementStrategy>(placementStrategy).has_value();
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

    const std::string DEFAULT_PLACEMENT_STRATEGY_TYPE = "NONE";
    const std::string DEFAULT_TOLERANCE_TYPE = "NONE";

    RequestHandlerServicePtr requestHandlerService;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    GlobalQueryPlanPtr globalQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    ErrorHandlerPtr errorHandler;
};
}// namespace REST::Controller

}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_REST_CONTROLLER_QUERYCONTROLLER_HPP_
