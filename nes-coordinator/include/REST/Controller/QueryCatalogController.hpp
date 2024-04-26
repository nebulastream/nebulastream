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
#ifndef NES_COORDINATOR_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_COORDINATOR_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#include <Catalogs/Exceptions/QueryNotFoundException.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Util/PlanJsonGenerator.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <utility>
#include OATPP_CODEGEN_BEGIN(ApiController)
#include <Catalogs/Query/DecomposedQueryPlanMetaData.hpp>

namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class ErrorHandler;
using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST::Controller {
class QueryCatalogController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param queryCatalog - queryCatalogService
     * @param coordinator - central entity of NebulaStream
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     * @param errorHandler - responsible for handling errors
     */
    QueryCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper,
                           const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                           const NesCoordinatorWeakPtr& coordinator,
                           const oatpp::String& completeRouterPrefix,
                           const ErrorHandlerPtr& errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), queryCatalog(queryCatalog),
          coordinator(coordinator), errorHandler(errorHandler) {}

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
                                                          const Catalogs::Query::QueryCatalogPtr& queryCatalog,
                                                          const NesCoordinatorWeakPtr& coordinator,
                                                          const std::string& routerPrefixAddition,
                                                          const ErrorHandlerPtr& errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<QueryCatalogController>(objectMapper,
                                                        queryCatalog,
                                                        coordinator,
                                                        completeRouterPrefix,
                                                        errorHandler);
    }

    ENDPOINT("GET", "/allRegisteredQueries", getAllRegisteredQueires) {
        try {
            auto response = queryCatalog->getAllQueryEntries();
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
            auto response = queryCatalog->getQueryEntriesWithStatus(status);
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
            auto response = queryCatalog->getQueryEntry(QueryId(queryId));
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
            auto sharedQueryId = queryCatalog->getLinkedSharedQueryId(QueryId(queryId));
            if (sharedQueryId == INVALID_SHARED_QUERY_ID) {
                return errorHandler->handleError(Status::CODE_404, "no query found with ID: " + std::to_string(queryId));
            }
            uint64_t processedBuffers = 0;
            if (auto shared_back_reference = coordinator.lock()) {
                std::vector<Runtime::QueryStatisticsPtr> statistics =
                    shared_back_reference->getQueryStatistics(UNSURE_CONVERSION_TODO_4761(sharedQueryId, QueryId));
                if (statistics.empty()) {
                    return errorHandler->handleError(Status::CODE_404,
                                                     "no statistics available for query with ID: " + std::to_string(queryId));
                }
                processedBuffers =
                    shared_back_reference->getQueryStatistics(UNSURE_CONVERSION_TODO_4761(sharedQueryId, QueryId))[0]
                        ->getProcessedBuffers();
            }
            nlohmann::json response;
            response["producedBuffers"] = processedBuffers;
            return createResponse(Status::CODE_200, response.dump());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

  private:
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    NesCoordinatorWeakPtr coordinator;
    ErrorHandlerPtr errorHandler;
};

}//namespace REST::Controller
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)

#endif // NES_COORDINATOR_INCLUDE_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
