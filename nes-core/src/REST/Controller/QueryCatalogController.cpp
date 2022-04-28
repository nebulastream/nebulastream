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

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Components/NesCoordinator.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/Controller/QueryCatalogController.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpprest/json.h>
#include <utility>

namespace NES {

QueryCatalogController::QueryCatalogController(QueryCatalogServicePtr queryCatalogService,
                                               NesCoordinatorWeakPtr coordinator,
                                               GlobalQueryPlanPtr globalQueryPlan)
    : queryCatalogService(std::move(queryCatalogService)), coordinator(std::move(coordinator)),
      globalQueryPlan(std::move(globalQueryPlan)) {
    NES_DEBUG("QueryCatalogController()");
}
#ifndef NES_USE_OATPP
void QueryCatalogController::handleGet(const std::vector<utility::string_t>& path, web::http::http_request& request) {

    //Extract parameters if any
    auto parameters = getParameters(request);

    if (path[1] == "queryIdAndCatalogEntryMapping") {
        //Check if the path contains the query id
        auto param = parameters.find("status");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            web::json::value errorResponse{};
            errorResponse["detail"] = web::json::value::string("Parameter queryId must be provided");
            errorMessageImpl(request, errorResponse);
        }

        try {
            //Prepare Input query from user string
            std::string queryStatus = param->second;

            //Prepare the response
            web::json::value result{};
            std::map<uint64_t, std::string> queries = queryCatalogService->getAllQueriesInStatus(queryStatus);

            for (auto [key, value] : queries) {
                result[key] = web::json::value::string(value);
            }

            if (queries.empty()) {
                NES_DEBUG("QueryCatalogController: handleGet -queryIdAndCatalogEntryMapping: no registered query with status "
                          + queryStatus + " was found.");
                noContentImpl(request);
            } else {
                successMessageImpl(request, result);
            }
            return;
        } catch (const std::exception& exc) {
            NES_ERROR("QueryCatalogController: handleGet -queryIdAndCatalogEntryMapping: Exception occurred while building the "
                      "query plan for "
                      "user request:"
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            NES_ERROR("QueryCatalogController: unknown exception.");
            internalServerErrorImpl(request);
        }
    } else if (path[1] == "allRegisteredQueries") {
        try {
            //Prepare the response
            web::json::value result{};
            std::map<uint64_t, QueryCatalogEntryPtr> queryCatalogEntries = queryCatalogService->getAllQueryCatalogEntries();

            uint64_t index = 0;
            for (auto& [queryId, catalogEntry] : queryCatalogEntries) {
                web::json::value jsonEntry;
                jsonEntry["queryId"] = web::json::value::number(queryId);
                jsonEntry["queryString"] = web::json::value::string(catalogEntry->getQueryString());
                jsonEntry["queryStatus"] = web::json::value::string(catalogEntry->getQueryStatusAsString());
                jsonEntry["queryPlan"] = PlanJsonGenerator::getQueryPlanAsJson(catalogEntry->getInputQueryPlan());
                jsonEntry["queryInfo"] = web::json::value::string(catalogEntry->getMetaInformation());
                result[index] = jsonEntry;
                index++;
            }

            if (queryCatalogEntries.empty()) {
                NES_DEBUG("QueryCatalogController: handleGet -queryIdAndCatalogEntryMapping: no registered query was found.");
                noContentImpl(request);
            } else {
                successMessageImpl(request, result);
            }
            return;
        } catch (const std::exception& exc) {
            NES_ERROR("QueryCatalogController: handleGet -allRegisteredQueries: Exception occurred while building the "
                      "query plan for user request:"
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            NES_ERROR("QueryCatalogController: unknown exception.");
            internalServerErrorImpl(request);
        }

    } else if (path[1] == "getNumberOfProducedBuffers") {
        //Check if the path contains the query id
        auto param = parameters.find("queryId");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            web::json::value errorResponse{};
            errorResponse["detail"] = web::json::value::string("Parameter queryId must be provided");
            errorMessageImpl(request, errorResponse);
        }

        try {
            NES_DEBUG("getNumberOfProducedBuffers called");
            //Prepare Input query from user string
            std::string queryId = param->second;
            SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryId(std::stoi(queryId));

            //Prepare the response
            web::json::value result{};
            uint64_t processedBuffers = 0;
            if (auto shared_back_reference = coordinator.lock()) {
                processedBuffers = shared_back_reference->getQueryStatistics(sharedQueryId)[0]->getProcessedBuffers();
            }
            NES_DEBUG("getNumberOfProducedBuffers processedBuffers=" << processedBuffers);

            result["producedBuffers"] = processedBuffers;

            successMessageImpl(request, result);
            return;
        } catch (const std::exception& exc) {
            NES_ERROR("QueryCatalogController: handleGet -getNumberOfProducedBuffers: Exception occurred while fetching "
                      "the number of buffers:"
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            NES_ERROR("QueryCatalogController: unknown exception.");
            internalServerErrorImpl(request);
        }
    } else if (path[1] == "status") {
        //Check if the path contains the query id
        auto param = parameters.find("queryId");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            web::json::value errorResponse{};
            errorResponse["detail"] = web::json::value::string("Parameter queryId must be provided");
            errorMessageImpl(request, errorResponse);
        }

        try {
            NES_DEBUG("Get current status of the query");
            //Prepare Input query from user string
            std::string queryId = param->second;

            //Prepare the response
            web::json::value result{};
            const QueryCatalogEntryPtr queryCatalogEntry = queryCatalogService->getEntryForQuery(std::stoi(queryId));
            std::string currentQueryStatus = queryCatalogEntry->getQueryStatusAsString();
            NES_DEBUG("Current query status=" << currentQueryStatus);

            result["status"] = web::json::value::string(currentQueryStatus);
            successMessageImpl(request, result);
            return;
        } catch (const std::exception& exc) {
            NES_ERROR(
                "QueryCatalogController: handleGet -status: Exception occurred while fetching the query status:" << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            NES_ERROR("QueryCatalogController: unknown exception.");
            internalServerErrorImpl(request);
        }
    } else {
        resourceNotFoundImpl(request);
    }
}
#endif
}// namespace NES
