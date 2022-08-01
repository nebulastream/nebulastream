//
// Created by balint on 29.07.22.
//

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
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/DTOs/ErrorMessage.hpp>
#include <REST/DTOs/QueryCatalogControllerCollectionResponse.hpp>
#include <REST/DTOs/QueryCatalogResponse.hpp>
#include <REST/DTOs/QueryCatalogControllerNumberOfProducedBuffersResponse.hpp>
#include <Services/QueryCatalogService.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/QueryStatistics.hpp>
//#include <REST/Handlers/ErrorHandler.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController)


namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

//class ErrorHandler;
//using ErrorHandlerPtr = std::shared_ptr<ErrorHandler>;

namespace REST {
namespace Controller {
class QueryCatalogController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     */
    QueryCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper,
                           QueryCatalogServicePtr queryCatalogService,
                           NesCoordinatorWeakPtr coordinator,
                           GlobalQueryPlanPtr globalQueryPlan,
                           oatpp::String completeRouterPrefix)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix),
          queryCatalogService(queryCatalogService),coordinator(coordinator), globalQueryPlan(globalQueryPlan){
            //errorHandler = std::make_shared<ErrorHandler>(objectMapper);
    }

    /**
     * Create a shared object of the API controller
     * @param objectMapper
     * @return
     */
    static std::shared_ptr<QueryCatalogController> createShared(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                                QueryCatalogServicePtr queryCatalogService,
                                                                NesCoordinatorWeakPtr coordinator,
                                                                GlobalQueryPlanPtr globalQueryPlan,
                                                                std::string routerPrefixAddition ) {
        oatpp::String completeRouterPrefix = baseRouterPrefix + routerPrefixAddition;
        return std::make_shared<QueryCatalogController>(objectMapper,
                                                        queryCatalogService,
                                                        coordinator,
                                                        globalQueryPlan,
                                                        completeRouterPrefix);
    }

    ENDPOINT("GET", "/allRegisteredQueries", getAllRegisteredQueires) {
        auto dto = QueryCatalogControllerCollectionResponse::createShared();
        try{
        std::map<uint64_t, QueryCatalogEntryPtr> queryCatalogEntries = queryCatalogService->getAllQueryCatalogEntries();

        uint64_t count = 0;
        for (auto& [queryId, catalogEntry] : queryCatalogEntries) {
            auto entry = QueryInfo::createShared();
            entry->queryId = queryId;
            entry->queryString = catalogEntry->getQueryString();
            entry->queryStatus = catalogEntry->getQueryStatusAsString();
            entry->queryPlan = catalogEntry->getInputQueryPlan()->toString();
            entry->queryMetaData = catalogEntry->getMetaInformation();
            //result[count] = jsonEntry;
            count++;
            dto->queries->push_back(entry);
        }
        if(count == 0){
            auto errorMessage = ErrorMessage::createShared();
            errorMessage->code = Status::CODE_204.code;
            errorMessage->status = "No registered queries";
            errorMessage->message = "";
            return createDtoResponse(Status::CODE_204, errorMessage);
            //return errorHandler->handleError(Status::CODE_204, "No registered queries");
        }
        return createDtoResponse(Status::CODE_200, dto);
        }
        catch(...){
            auto errorMessage = ErrorMessage::createShared();
            errorMessage->code = Status::CODE_500.code;
            errorMessage->status = "Internal Error";
            errorMessage->message = "";
            return createDtoResponse(Status::CODE_500, errorMessage);
        }
    }

    ENDPOINT("GET", "/queries", getQueriesWithASpecificStatus, QUERY(String, status, "status")){
        try {
            std::map<uint64_t, std::string> queries = queryCatalogService->getAllQueriesInStatus(status);
            auto dto = QueryCatalogControllerCollectionResponse::createShared();
            uint64_t count = 0;
            for (auto [key, value] : queries) {
                auto entry = QueryInfo::createShared();
                entry->queryId = key;
                entry->queryString = value;
                count++;
                dto->queries->push_back(entry);
            }
            if (count == 0) {
                auto errorMessage = ErrorMessage::createShared();
                errorMessage->code = Status::CODE_204.code;
                errorMessage->status = "No queries with status: " + status + " found";
                errorMessage->message = "";
                return createDtoResponse(Status::CODE_204, errorMessage);
                //return errorHandler->handleError(Status::CODE_204, "No registered queries");
            }
            return createDtoResponse(Status::CODE_200, dto);
        }
        catch(...){
            auto errorMessage = ErrorMessage::createShared();
            errorMessage->code = Status::CODE_500.code;
            errorMessage->status = "Internal Error";
            errorMessage->message = "";
            return createDtoResponse(Status::CODE_500, errorMessage);
        }
    }

    ENDPOINT("GET", "/status", getStatusOfQuery, QUERY(UInt64 , queryId, "queryId")) {
        try {
            NES_DEBUG("Get current status of the query");
            const QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(queryId);
            std::string currentQueryStatus = queryCatalogEntry->getQueryStatusAsString();
            NES_DEBUG("Current query status=" << currentQueryStatus);
            auto entry = QueryInfo::createShared();
            entry->queryId = queryId;
            entry->queryStatus = currentQueryStatus;
            return createDtoResponse(Status::CODE_200, entry);
        } catch (QueryNotFoundException e ) {
            auto errorMessage = ErrorMessage::createShared();
            errorMessage->code = Status::CODE_204.code;
            errorMessage->status = "No query with given ID";
            errorMessage->message = e.what();
            return createDtoResponse(Status::CODE_204, errorMessage);
        }
        catch (...) {
            auto errorMessage = ErrorMessage::createShared();
            errorMessage->code = Status::CODE_500.code;
            errorMessage->status = "Internal Error";
            errorMessage->message = "";
            return createDtoResponse(Status::CODE_500, errorMessage);
        }
    }

    ENDPOINT("GET", "/getNumberOfProducedBuffers", getNumberOfProducedBuffers, QUERY(UInt64 , queryId, "queryId")) {
        try {
            NES_DEBUG("getNumberOfProducedBuffers called");
            //Prepare Input query from user string
            SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryId(queryId);


            uint64_t processedBuffers = 0;
            if (auto shared_back_reference = coordinator.lock()) {
                processedBuffers = shared_back_reference->getQueryStatistics(sharedQueryId)[0]->getProcessedBuffers();
            }
            auto result = QueryCatalogControllerNumberOfProducedBuffersResponse::createShared();
            result->producedBuffers = processedBuffers;
            return createDtoResponse(Status::CODE_200, result);
        } catch (...) {
            auto errorMessage = ErrorMessage::createShared();
            errorMessage->code = Status::CODE_500.code;
            errorMessage->status = "Internal Error";
            errorMessage->message = "";
            return createDtoResponse(Status::CODE_500, errorMessage);
        }
    }

  private:
    QueryCatalogServicePtr queryCatalogService;
    NesCoordinatorWeakPtr coordinator;
    GlobalQueryPlanPtr globalQueryPlan;
    //ErrorHandlerPtr errorHandler;
};
}//namespace Controller
}// namespace REST
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)

#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCATALOGCONTROLLER_HPP_
