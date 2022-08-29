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
#ifndef NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCONTROLLER_HPP_
#define NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCONTROLLER_HPP_
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <REST/DTOs/ErrorResponse.hpp>
#include <REST/DTOs/QueryControllerResponse.hpp>
#include <REST/DTOs/QueryControllerOptimizationPhasesResponse.hpp>
#include <REST/DTOs/QueryControllerSubmitQueryRequest.hpp>
#include <REST/DTOs/QueryControllerSubmitQueryResponse.hpp>
#include <REST/DTOs/QueryControllerStopQueryResponse.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <REST/OatppController/BaseRouterPrefix.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <cpprest/json.h>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <Services/QueryService.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <exception>

#include OATPP_CODEGEN_BEGIN(ApiController)
#include "GRPC/Serialization/QueryPlanSerializationUtil.hpp"
#include "SerializableQueryPlan.pb.h"

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
                    oatpp::String completeRouterPrefix,
                    ErrorHandlerPtr errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix),
          queryService(queryService),queryCatalogService(queryCatalogService), globalExecutionPlan(globalExecutionPlan),errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<QueryController> createShared(const std::shared_ptr<ObjectMapper>& objectMapper,
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

    ENDPOINT("GET", "/execution-plan", getExecutionPlan, QUERY(UInt64 , queryId, "queryId")) {
        try {
            NES_DEBUG("Get current status of the query");
            const Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            auto executionPlanJson = PlanJsonGenerator::getExecutionPlanAsJson(globalExecutionPlan, queryId).serialize(); //TODO: change when removing cpprestsdk
            NES_DEBUG("QueryController:: execution-plan: " << executionPlanJson);
            auto dto = DTO::QueryControllerResponse::createShared();
            dto->entry = executionPlanJson;
            return createDtoResponse(Status::CODE_200, dto);
        } catch (QueryNotFoundException e ) {
            return errorHandler->handleError(Status::CODE_204, "No query with given ID: " + std::to_string(queryId));
        }
        catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/query-plan", getQueryPlan, QUERY(UInt64 , queryId, "queryId")) {
        try {
            NES_DEBUG("Get current status of the query");
            const Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");
            auto basePlan = PlanJsonGenerator::getQueryPlanAsJson(queryCatalogEntry->getInputQueryPlan()).serialize();
            auto dto = DTO::QueryControllerResponse::createShared();
            dto->entry = basePlan;
            return createDtoResponse(Status::CODE_200, dto);
        } catch (QueryNotFoundException e ) {
            return errorHandler->handleError(Status::CODE_204, "No query with given ID: " + std::to_string(queryId));
        }
        catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/optimization-phase", getOptimizationPhase, QUERY(UInt64 , queryId, "queryId")) {
        try {
            oatpp::Fields<String> map({});
            const Catalogs::Query::QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");
            auto optimizationPhases = queryCatalogEntry->getOptimizationPhases();
            for (auto const& [phaseName, queryPlan] : optimizationPhases) {
                auto queryPlanString = PlanJsonGenerator::getQueryPlanAsJson(queryPlan).serialize();
                map->push_back({phaseName, queryPlanString});
            }
            auto dto = REST::DTO::QueryControllerOptimizationPhasesResponse::createShared();
            dto->optimizationPhases = map;
            return createDtoResponse(Status::CODE_200, dto);
        } catch (QueryNotFoundException e ) {
            return errorHandler->handleError(Status::CODE_204, "No query with given ID: " + std::to_string(queryId));
        }
        catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }
    ENDPOINT("GET", "/query-status", getQueryStatus, QUERY(UInt64 ,queryId, "queryId")) {
        //NOTE: QueryController has "query-status" endpoint. QueryCatalogController has "status" endpoint with same functionality.
        //No sense in duplicating functionality, so "query-status" endpoint is not implemented.
       return errorHandler->handleError(Status::CODE_301, "Requests for the status of a query are now served at /queryCatalogController/status?queryId=" + std::to_string(queryId));
    }

    ENDPOINT("POST", "/execute-query", submitQuery, BODY_DTO(Object<DTO::QueryControllerSubmitQueryRequest>, request)) {
        try {
            if(!validatePlacementStrategy(request->placement)){
                std::string errorMessage =  "Invalid Placement Strategy: " + request->placement +
                    ". Further info can be found at https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html";
                return errorHandler->handleError(Status::CODE_400,errorMessage);
            }
            if(!validateFaultToleranceType(request->faultTolerance)){
                std::string errorMessage = "Invalid fault tolerance Type provided: " + request->faultTolerance +
                    ". Valid Fault Tolerance Types are: 'AT_MOST_ONCE', 'AT_LEAST_ONCE', 'EXACTLY_ONCE', 'NONE'.";
                return errorHandler->handleError(Status::CODE_400, errorMessage);
            }
            auto faultToleranceMode = stringToFaultToleranceTypeMap(request->faultTolerance);
            auto lineageMode = stringToLineageTypeMap(request->lineage);
            NES_DEBUG("QueryController: handlePost -execute-query: Params: userQuery= "
                      << request->userQuery.getValue("null") << ", strategyName= " << request->placement.getValue("null")
                      << ", faultTolerance= " << request->faultTolerance.getValue("null") << ", lineage= " << request->lineage.getValue("null"));
            QueryId queryId = queryService->validateAndQueueAddQueryRequest(request->userQuery,
                                                                            request->placement,
                                                                            faultToleranceMode,
                                                                            lineageMode);
            //Prepare the response
            auto dto = DTO::QueryControllerSubmitQueryResponse::createShared();
            dto->queryId = queryId;
            return createDtoResponse(Status::CODE_200, dto);
        }
        catch (const InvalidQueryException& exc) {
            NES_ERROR("QueryController: handlePost -execute-query: Exception occurred during submission of a query "
                      "user request:"
                      << exc.what());
            return errorHandler->handleError(Status::CODE_400, exc.what());
        }
        catch (std::exception& e) {
            std::string errorMessage = "Potentially incorrect or missing key words for user query (use 'userQuery') or placement strategy (use 'strategyName')."
                "For more info check: https://docs.nebula.stream/docs/clients/rest-api/ \n";
            return errorHandler->handleError(Status::CODE_500, errorMessage + e.what());
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
            std::string faultToleranceString = DEFAULT_TOLERANCE_TYPE;
            std::string lineageString = DEFAULT_TOLERANCE_TYPE;
            if (context->contains("faultTolerance")) {
                if (!validateFaultToleranceType(context->at("faultTolerance").value())) {
                    std::string errorMessage = "Invalid fault tolerance Type provided: " + context->at("faultTolerance").value() +
                        ". Valid Fault Tolerance Types are: 'AT_MOST_ONCE', 'AT_LEAST_ONCE', 'EXACTLY_ONCE', 'NONE'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);
                } else {
                    faultToleranceString = context->at("faultTolerance").value();
                }
            }
            if (context->contains("lineage")) {
                if (!validateLineageMode(lineageString = context->at("lineage").value())) {
                    NES_ERROR("QueryController: handlePost -execute-query: Invalid Lineage Type provided: " + lineageString);
                        std::string errorMessage = "Invalid Lineage Mode Type provided: " + lineageString
                                                 + ". Valid Lineage Modes are: 'IN_MEMORY', 'PERSISTENT', 'REMOTE', 'NONE'.";
                    return errorHandler->handleError(Status::CODE_400, errorMessage);
                } else {
                    lineageString = context->at("lineage").value();
                }
            }
            std::string* queryString = protobufMessage->mutable_querystring();
            std::string placementStrategy = context->at("placement").value();
            auto faultToleranceMode = stringToFaultToleranceTypeMap(faultToleranceString);
            auto lineageMode = stringToLineageTypeMap(lineageString);
            QueryId queryId = queryService->addQueryRequest(*queryString,
                                                            queryPlan,
                                                            placementStrategy,
                                                            faultToleranceMode,
                                                            lineageMode);

            //Prepare the response
            auto dto = DTO::QueryControllerSubmitQueryResponse::createShared();
            dto->queryId = queryId;
            return createDtoResponse(Status::CODE_201, dto);
        } catch (const std::exception& exc) {
            NES_ERROR(
                "QueryController: handlePost -execute-query-ex: Exception occurred while building the query plan for "
                "user request:"
                << exc.what());
            return errorHandler->handleError(Status::CODE_400, exc.what());
        } catch (...) {
            NES_ERROR("RestServer: unknown exception.");
            return errorHandler->handleError(Status::CODE_500, "unknown exception");
        }
    }

    ENDPOINT("DELETE", "/stop-query", stopQuery, QUERY(UInt64, queryId, "queryId")){
        try{
            bool success = queryService->validateAndQueueStopQueryRequest(queryId);
            Status status = success ? Status::CODE_202 : Status::CODE_400;  //QueryController catches InvalidQueryStatus exception, but this is never thrown since it was commented out
            auto dto = DTO::QueryControllerStopQueryResponse::createShared();
            dto->success = success;
            return createDtoResponse(status,dto);
        }
        catch (QueryNotFoundException e) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        }
        catch (...) {
            NES_ERROR("RestServer: unknown exception.");
            return errorHandler->handleError(Status::CODE_500, "unknown exception");
        }
    }

  private:

    bool validatePlacementStrategy(const std::string& placementStrategy) {
        try {
            PlacementStrategy::getFromString(placementStrategy);
        } catch (Exceptions::RuntimeException exc) {
            return false;
        }
        return true;
    }

    bool validateFaultToleranceType(const std::string& faultToleranceString) {
        auto faultToleranceMode = stringToFaultToleranceTypeMap(faultToleranceString);
        if (faultToleranceMode == FaultToleranceType::INVALID) {
            NES_ERROR("QueryController: handlePost -execute-query: Invalid Fault Tolerance Type provided: " + faultToleranceString);
            return false;
        }
        return true;
    }

    std::optional<std::shared_ptr<oatpp::web::protocol::http::outgoing::Response>>
        validateProtobufMessage(const std::shared_ptr<SubmitQueryRequest>& protobufMessage,
                                                  const utility::string_t& body) {
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
        if(!validatePlacementStrategy(placementStrategy)){
            std::string errorMessage =  "Invalid Placement Strategy: " + placementStrategy +
                ". Further info can be found at https://docs.nebula.stream/cpp/class_n_e_s_1_1_placement_strategy.html";
            return errorHandler->handleError(Status::CODE_400,errorMessage);
        }
        return std::nullopt;
    }

    bool validateLineageMode(const std::string& lineageModeString) {
        auto lineageMode = stringToLineageTypeMap(lineageModeString);
        if (lineageMode == LineageType::INVALID) {
            return false;
        }
        return true;
    }

    QueryServicePtr queryService;
    QueryCatalogServicePtr queryCatalogService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    ErrorHandlerPtr errorHandler;

};
}//namespace Controller
}// namespace REST
}// namespace NES
#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCONTROLLER_HPP_
