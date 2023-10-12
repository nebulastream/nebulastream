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
#ifndef NES_CORE_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_CORE_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>
#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST {
namespace Controller {
class QueryCatalogController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param queryCatalogService - queryCatalogService
     * @param coordinator - central entity of NebulaStream
     * @param globalQueryPlan - responsible for storing all currently running and to be deployed QueryPlans in the NES system.
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     * @param errorHandler - responsible for handling errors
     */
    QueryCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper,
                           const QueryCatalogServicePtr& queryCatalogService,
                           const NesCoordinatorWeakPtr& coordinator,
                           const GlobalQueryPlanPtr& globalQueryPlan,
                           const oatpp::String& completeRouterPrefix,
                           const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), queryCatalogService(queryCatalogService),
          coordinator(coordinator), globalQueryPlan(globalQueryPlan), errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param queryCatalogService - queryCatalogService
     * @param coordinator - central entity of NebulaStream
     * @param globalQueryPlan - responsible for storing all currently running and to be deployed QueryPlans in the NES system.
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @param errorHandler - responsible for handling errors
     */
    static std::shared_ptr<QueryCatalogController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                          const QueryCatalogServicePtr& queryCatalogService,
                                                          const NesCoordinatorWeakPtr& coordinator,
                                                          const GlobalQueryPlanPtr& globalQueryPlan,
                                                          const std::string& routerPrefixAddition,
                                                          const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<QueryCatalogController>(objectMapper,
                                                        queryCatalogService,
                                                        coordinator,
                                                        globalQueryPlan,
                                                        completeRouterPrefix,
                                                        errorHandler);
    }

    ENDPOINT("GET", "/allRegisteredQueries", getAllRegisteredQueires) {
        nlohmann::json response;
        try {
            std::map<uint64_t, Catalogs::Query::QueryCatalogEntryPtr> queryCatalogEntries =
                queryCatalogService->getAllQueryCatalogEntries();
            for (auto& [queryId, catalogEntry] : queryCatalogEntries) {
                nlohmann::json entry;
                entry["queryId"] = queryId;
                entry["queryString"] = catalogEntry->getQueryString();
                entry["queryStatus"] = catalogEntry->getQueryStatusAsString();
                entry["queryPlan"] = catalogEntry->getInputQueryPlan()->toString();
                entry["queryMetaData"] = catalogEntry->getMetaInformation();
                response.push_back(entry);
            }
            return createResponse(Status::CODE_200, response.dump());
        } catch (const std::exception& exc) {
            NES_ERROR("QueryCatalogController: handleGet -allRegisteredQueries: Exception occurred while building the "
                      "query plan for user request: {}",
                      exc.what());
            return errorHandler->handleError(Status::CODE_400,
                                             "Exception occurred while building query plans for user request"
                                                 + std::string(exc.what()));
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/queries", getQueriesWithASpecificStatus, QUERY(String, status, "status")) {
        try {
            std::map<uint64_t, std::string> queries = queryCatalogService->getAllQueriesInStatus(status);
            nlohmann::json response;
            for (auto [key, value] : queries) {
                nlohmann::json entry;
                auto catalogEntry = queryCatalogService->getEntryForQuery(key);
                entry["queryId"] = key;
                entry["queryString"] = catalogEntry->getQueryString();
                entry["queryStatus"] = catalogEntry->getQueryStatusAsString();
                entry["queryPlan"] = catalogEntry->getInputQueryPlan()->toString();
                entry["queryMetaData"] = catalogEntry->getMetaInformation();
                response.push_back(entry);
            }
            return createResponse(Status::CODE_200, response.dump());
        } catch (InvalidArgumentException e) {
            return errorHandler->handleError(Status ::CODE_400, "Invalid Status provided");
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/status", getStatusOfQuery, QUERY(UInt64, queryId, "queryId")) {
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

    ENDPOINT("GET", "/getNumberOfProducedBuffers", getNumberOfProducedBuffers, QUERY(UInt64, queryId, "queryId")) {
        try {
            NES_DEBUG("getNumberOfProducedBuffers called");
            //Prepare Input query from user string
            SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
            if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
                return errorHandler->handleError(Status::CODE_404, "no query found with ID: " + std::to_string(queryId));
            }
            uint64_t processedBuffers = 0;
            if (auto shared_back_reference = coordinator.lock()) {
                std::vector<Runtime::QueryStatisticsPtr> statistics = shared_back_reference->getQueryStatistics(sharedQueryId);
                if (statistics.empty()) {
                    return errorHandler->handleError(Status::CODE_404,
                                                     "no statistics available for query with ID: " + std::to_string(queryId));
                }
                processedBuffers = shared_back_reference->getQueryStatistics(sharedQueryId)[0]->getProcessedBuffers();
            }
            nlohmann::json response;
            response["producedBuffers"] = processedBuffers;
            return createResponse(Status::CODE_200, response.dump());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

  private:
    QueryCatalogServicePtr queryCatalogService;
    NesCoordinatorWeakPtr coordinator;
    GlobalQueryPlanPtr globalQueryPlan;
    ErrorHandlerPtr errorHandler;
};

}//namespace Controller
}// namespace REST
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)

#endif// NES_CORE_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
