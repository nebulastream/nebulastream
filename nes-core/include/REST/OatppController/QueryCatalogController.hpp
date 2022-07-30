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
#include <REST/DTOs/QueryCatalogControllerAllRegisteredQueriesResponse.hpp>
#include <REST/DTOs/QueryCatalogResponse.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/DTOs/ErrorMessage.hpp>

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {
class NesCoordinator;
using NesCoordinatorWeakPtr = std::weak_ptr<NesCoordinator>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;
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
          queryCatalogService(queryCatalogService),coordinator(coordinator), globalQueryPlan(globalQueryPlan){}

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

    ENDPOINT("GET", "/allRegisteredQueries", root) {
        auto dto = QueryCatalogControllerAllRegisteredQueriesResponse::createShared();
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
        }
        return createDtoResponse(Status::CODE_200, dto);

    }

  private:
    QueryCatalogServicePtr queryCatalogService;
    NesCoordinatorWeakPtr coordinator;
    GlobalQueryPlanPtr globalQueryPlan;
};
}//namespace Controller
}// namespace REST
}// namespace NES

#include OATPP_CODEGEN_END(ApiController)

#endif//NES_NES_CORE_INCLUDE_REST_OATPPCONTROLLER_QUERYCATALOGCONTROLLER_HPP_
