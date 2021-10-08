/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/Controller/QueryCatalogController.hpp>
#include <REST/runtime_utils.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES {

QueryCatalogController::QueryCatalogController(QueryCatalogPtr queryCatalog,
                                               NesCoordinatorWeakPtr coordinator,
                                               GlobalQueryPlanPtr globalQueryPlan)
    : queryCatalog(std::move(queryCatalog)), coordinator(std::move(coordinator)), globalQueryPlan(std::move(globalQueryPlan)) {
    NES_DEBUG("QueryCatalogController()");
}

void QueryCatalogController::handleGet(std::vector<utility::string_t> path, web::http::http_request request) {

    //Extract parameters if any
    auto parameters = getParameters(request);

    if (path[1] == "queries") {
        //Check if the path contains the query id
        auto param = parameters.find("status");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter queryId must be provided");
            badRequestImpl(request, errorResponse);
        }

        try {
            //Prepare Input query from user string
            std::string queryStatus = param->second;

            //Prepare the response
            json::value result{};
            std::map<uint64_t, std::string> queries = queryCatalog->getQueriesWithStatus(queryStatus);

            for (auto [key, value] : queries) {
                result[key] = json::value::string(value);
            }

            if (queries.empty()) {
                NES_DEBUG("QueryCatalogController: handleGet -queries: no registered query with status " + queryStatus
                          + " was found.");
                noContentImpl(request);
            } else {
                successMessageImpl(request, result);
            }
            return;
        } catch (const std::exception& exc) {
            NES_ERROR("QueryCatalogController: handleGet -queries: Exception occurred while building the query plan for "
                      "user request:"
                      << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
        }
    } else if (path[1] == "allRegisteredQueries") {
        try {
            //Prepare the response
            json::value result{};
            std::map<uint64_t, QueryCatalogEntryPtr> queryCatalogEntries = queryCatalog->getAllQueryCatalogEntries();

            uint64_t index = 0;
            for (auto& [queryId, catalogEntry] : queryCatalogEntries) {
                json::value jsonEntry;
                jsonEntry["queryId"] = json::value::number(queryId);
                jsonEntry["queryString"] = json::value::string(catalogEntry->getQueryString());
                jsonEntry["queryStatus"] = json::value::string(catalogEntry->getQueryStatusAsString());
                jsonEntry["queryPlan"] = PlanJsonGenerator::getQueryPlanAsJson(catalogEntry->getInputQueryPlan());
                jsonEntry["queryInfo"] = json::value::string(catalogEntry->getFailureReason());
                result[index] = jsonEntry;
                index++;
            }

            if (queryCatalogEntries.empty()) {
                NES_DEBUG("QueryCatalogController: handleGet -queries: no registered query was found.");
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
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
        }

    } else if (path[1] == "getNumberOfProducedBuffers") {
        //Check if the path contains the query id
        auto param = parameters.find("queryId");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter queryId must be provided");
            badRequestImpl(request, errorResponse);
        }

        try {
            NES_DEBUG("getNumberOfProducedBuffers called");
            //Prepare Input query from user string
            std::string queryId = param->second;
            SharedQueryId sharedQueryId = globalQueryPlan->getSharedQueryId(std::stoi(queryId));

            //Prepare the response
            json::value result{};
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
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
        }
    } else if (path[1] == "status") {
        //Check if the path contains the query id
        auto param = parameters.find("queryId");
        if (param == parameters.end()) {
            NES_ERROR("QueryController: Unable to find query ID for the GET execution-plan request");
            json::value errorResponse{};
            errorResponse["detail"] = json::value::string("Parameter queryId must be provided");
            badRequestImpl(request, errorResponse);
        }

        try {
            NES_DEBUG("Get current status of the query");
            //Prepare Input query from user string
            std::string queryId = param->second;

            //Prepare the response
            json::value result{};
            const QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(std::stoi(queryId));
            std::string currentQueryStatus = queryCatalogEntry->getQueryStatusAsString();
            NES_DEBUG("Current query status=" << currentQueryStatus);

            result["status"] = json::value::string(currentQueryStatus);
            successMessageImpl(request, result);
            return;
        } catch (const std::exception& exc) {
            NES_ERROR(
                "QueryCatalogController: handleGet -status: Exception occurred while fetching the query status:" << exc.what());
            handleException(request, exc);
            return;
        } catch (...) {
            RuntimeUtils::printStackTrace();
            internalServerErrorImpl(request);
        }
    } else {
        resourceNotFoundImpl(request);
    }
}

}// namespace NES
