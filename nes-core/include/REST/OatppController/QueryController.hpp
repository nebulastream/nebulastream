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
#include <REST/DTOs/ErrorMessage.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <cpprest/json.h>

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
        oatpp::String completeRouterPrefix = baseRouterPrefix + routerPrefixAddition;
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
            const QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            auto executionPlanJson = PlanJsonGenerator::getExecutionPlanAsJson(globalExecutionPlan, queryId).serialize(); //TODO: change when removing cpprestsdk
            NES_DEBUG("QueryController:: execution-plan: " << executionPlanJson);
            auto entry = QueryInfo::createShared();
            entry->queryId = queryId;
            entry->queryPlan = executionPlanJson;
            return createDtoResponse(Status::CODE_200, entry);
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
            const QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");
            auto basePlan = PlanJsonGenerator::getQueryPlanAsJson(queryCatalogEntry->getInputQueryPlan()).serialize();
            auto entry = QueryInfo::createShared();
            entry->queryId = queryId;
            entry->queryPlan = basePlan;
            return createDtoResponse(Status::CODE_200, entry);
        } catch (QueryNotFoundException e ) {
            return errorHandler->handleError(Status::CODE_204, "No query with given ID: " + std::to_string(queryId));
        }
        catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/optimization-phase", getOptimizationPhase, QUERY(UInt64 , queryId, "queryId")) {
        try {
            NES_DEBUG("Get current status of the query");
            const QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");
            auto basePlan = PlanJsonGenerator::getQueryPlanAsJson(queryCatalogEntry->getInputQueryPlan()).serialize();
            auto entry = QueryInfo::createShared();
            entry->queryId = queryId;
            entry->queryPlan = basePlan;
            return createDtoResponse(Status::CODE_200, entry);
        } catch (QueryNotFoundException e ) {
            return errorHandler->handleError(Status::CODE_204, "No query with given ID: " + std::to_string(queryId));
        }
        catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }


  private:

    QueryServicePtr queryService;
    QueryCatalogServicePtr queryCatalogService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    ErrorHandlerPtr errorHandler;

};
}//namespace Controller
}// namespace REST
}// namespace NES
#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCONTROLLER_HPP_
