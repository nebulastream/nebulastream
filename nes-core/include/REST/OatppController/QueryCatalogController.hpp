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
#ifndef NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/DTOs/BuffersProducedResponse.hpp>
#include <REST/DTOs/ErrorMessage.hpp>
#include <REST/DTOs/QueryCatalogEntriesResponse.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Services/QueryCatalogService.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
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
     * @param coordinator
     * @param globalQueryPlan
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     * @param errorHandler - responsible for handling errors
     */
    QueryCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper,
                           QueryCatalogServicePtr queryCatalogService,
                           NesCoordinatorWeakPtr coordinator,
                           GlobalQueryPlanPtr globalQueryPlan,
                           oatpp::String completeRouterPrefix,
                           ErrorHandlerPtr errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix),
          queryCatalogService(queryCatalogService),coordinator(coordinator), globalQueryPlan(globalQueryPlan), errorHandler(errorHandler){

    }

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param queryCatalogService - queryCatalogService
     * @param coordinator
     * @param globalQueryPlan
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @param errorHandler - responsible for handling errors
     */
    static std::shared_ptr<QueryCatalogController> createShared(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                                QueryCatalogServicePtr queryCatalogService,
                                                                NesCoordinatorWeakPtr coordinator,
                                                                GlobalQueryPlanPtr globalQueryPlan,
                                                                std::string routerPrefixAddition,
                                                                ErrorHandlerPtr errorHandler) {
        oatpp::String completeRouterPrefix = baseRouterPrefix + routerPrefixAddition;
        return std::make_shared<QueryCatalogController>(objectMapper,
                                                        queryCatalogService,
                                                        coordinator,
                                                        globalQueryPlan,
                                                        completeRouterPrefix,
                                                        errorHandler);
    }

    ENDPOINT("GET", "/allRegisteredQueries", getAllRegisteredQueires) {
        auto dto = DTO::QueryCatalogEntriesResponse::createShared();
        oatpp::List<oatpp::Object<DTO::QueryCatalogEntryResponse>> list({});
        try{
        std::map<uint64_t, QueryCatalogEntryPtr> queryCatalogEntries = queryCatalogService->getAllQueryCatalogEntries();
            for (auto& [queryId, catalogEntry] : queryCatalogEntries) {
            auto entry = DTO::QueryCatalogEntryResponse::createShared();
            entry->queryId = queryId;
            entry->queryString = catalogEntry->getQueryString();
            entry->queryStatus = catalogEntry->getQueryStatusAsString();
            entry->queryPlan = catalogEntry->getInputQueryPlan()->toString();
            entry->queryMetaData = catalogEntry->getMetaInformation();
            list->push_back(entry);
        }
        dto->queries= list;
        return createDtoResponse(Status::CODE_200, dto);
        }
        catch(...){
            return errorHandler->handleError(Status::CODE_500, "Internal Error");

        }
    }

    ENDPOINT("GET", "/queries", getQueriesWithASpecificStatus, QUERY(String, status, "status")){
        try {
            std::map<uint64_t, std::string> queries = queryCatalogService->getAllQueriesInStatus(status);
            auto dto = DTO::QueryCatalogEntriesResponse::createShared();
            oatpp::List<oatpp::Object<DTO::QueryCatalogEntryResponse>> list({});
            for (auto [key, value] : queries) {
                auto entry = DTO::QueryCatalogEntryResponse::createShared();
                auto catalogEntry = queryCatalogService->getEntryForQuery(key);
                entry->queryId = key;
                entry->queryString = catalogEntry->getQueryString();
                entry->queryStatus = catalogEntry->getQueryStatusAsString();
                entry->queryPlan = catalogEntry->getInputQueryPlan()->toString();
                entry->queryMetaData = catalogEntry->getMetaInformation();
                list->push_back(entry);
            }
            dto->queries=list;
            return createDtoResponse(Status::CODE_200, dto);
        }
        catch (InvalidArgumentException e){
            return errorHandler->handleError(Status ::CODE_400,"Invalid Status provided");
        }
        catch(...){
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/status", getStatusOfQuery, QUERY(UInt64 , queryId, "queryId")) {
        try {
            NES_DEBUG("Get current status of the query");
            const QueryCatalogEntryPtr catalogEntry = queryCatalogService->getEntryForQuery(queryId);
            auto entry = DTO::QueryCatalogEntryResponse::createShared();
            entry->queryId = queryId;
            entry->queryString = catalogEntry->getQueryString();
            entry->queryStatus = catalogEntry->getQueryStatusAsString();
            entry->queryPlan = catalogEntry->getInputQueryPlan()->toString();
            entry->queryMetaData = catalogEntry->getMetaInformation();
            return createDtoResponse(Status::CODE_200, entry);
        } catch (QueryNotFoundException e ) {
            return errorHandler->handleError(Status::CODE_404, "No query with given ID: " + std::to_string(queryId));
        }
        catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Error");
        }
    }

    ENDPOINT("GET", "/getNumberOfProducedBuffers", getNumberOfProducedBuffers, QUERY(UInt64 , queryId, "queryId")) {
        try {
            NES_DEBUG("getNumberOfProducedBuffers called");
            //Prepare Input query from user string
            SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);
            if(sharedQueryId == INVALID_SHARED_QUERY_ID){
                return errorHandler->handleError(Status::CODE_404, "no query found with ID: " + std::to_string(queryId));
            }
            uint64_t processedBuffers = 0;
            if (auto shared_back_reference = coordinator.lock()) {
                std::vector<Runtime::QueryStatisticsPtr> statistics = shared_back_reference->getQueryStatistics(sharedQueryId);
                if(statistics.empty()){
                    return errorHandler->handleError(Status::CODE_404, "no statistics available for query with ID: " + std::to_string(queryId));
                }
                processedBuffers = shared_back_reference->getQueryStatistics(sharedQueryId)[0]->getProcessedBuffers();
            }
            auto result = DTO::BuffersProducedResponse::createShared();
            result->producedBuffers = processedBuffers;
            return createDtoResponse(Status::CODE_200, result);
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

#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCATALOGCONTROLLER_HPP_
