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
#include <NodeEngine/QueryStatistics.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <REST/Controller/QueryCatalogController.hpp>
#include <REST/runtime_utils.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryCatalogController::QueryCatalogController(QueryCatalogPtr queryCatalog, NesCoordinatorWeakPtr coordinator, GlobalQueryPlanPtr globalQueryPlan)
    : queryCatalog(queryCatalog), coordinator(coordinator), globalQueryPlan(globalQueryPlan) {
    NES_DEBUG("QueryCatalogController()");
}

void QueryCatalogController::handleGet(std::vector<utility::string_t> path, web::http::http_request message) {
    if (path[1] == "queries") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    json::value req = json::value::parse(payload);
                    std::string queryStatus = req.at("status").as_string();

                    //Prepare the response
                    json::value result{};
                    std::map<uint64_t, std::string> queries = queryCatalog->getQueriesWithStatus(queryStatus);

                    for (auto [key, value] : queries) {
                        result[key] = json::value::string(value);
                    }

                    if (queries.size() == 0) {
                        NES_DEBUG(
                            "QueryCatalogController: handleGet -queries: no registered query with status " + queryStatus
                            + " was found.");
                        noContentImpl(message);
                    } else {
                        successMessageImpl(message, result);
                    }
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "QueryCatalogController: handleGet -queries: Exception occurred while building the query plan for user request:"
                        << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    } else if (path[1] == "allRegisteredQueries") {
        message.extract_string(true)
            .then([this, message](const utility::string_t&) {
                try {
                    //Prepare the response
                    json::value result{};
                    std::map<uint64_t, std::string> queries = queryCatalog->getAllQueries();

                    for (auto [key, value] : queries) {
                        result[key] = json::value::string(value);
                    }

                    if (queries.size() == 0) {
                        NES_DEBUG("QueryCatalogController: handleGet -queries: no registered query was found.");
                        noContentImpl(message);
                    } else {
                        successMessageImpl(message, result);
                    }
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "QueryCatalogController: handleGet -allRegisteredQueries: Exception occurred while building the query plan for user request:"
                        << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    } else if (path[1] == "getNumberOfProducedBuffers") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("getNumberOfProducedBuffers called");
                    //Prepare Input query from user string
                    std::string queryId(body.begin(), body.end());
                    NES_DEBUG("getNumberOfProducedBuffers payload=" << queryId);

                    GlobalQueryId globalQueryId = globalQueryPlan->getGlobalQueryIdForQuery(std::stoi(queryId));

                    //Prepare the response
                    json::value result{};
                    size_t processedBuffers = 0;
                    if (auto shared_back_reference = coordinator.lock()) {
                        processedBuffers = shared_back_reference->getQueryStatistics(globalQueryId)[0]->getProcessedBuffers();
                    }
                    NES_DEBUG("getNumberOfProducedBuffers processedBuffers=" << processedBuffers);

                    result["producedBuffers"] = processedBuffers;

                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(
                        "QueryCatalogController: handleGet -getNumberOfProducedBuffers: Exception occurred while fetching the number of buffers:"
                        << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    } else if (path[1] == "status") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    NES_DEBUG("Get current status of the query");
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    NES_DEBUG("status payload=" << payload);

                    //Prepare the response
                    json::value result{};
                    const QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(std::stoi(payload));
                    std::string currentQueryStatus = queryCatalogEntry->getQueryStatusAsString();
                    NES_DEBUG("Current query status=" << currentQueryStatus);

                    result["status"] = json::value::string(currentQueryStatus);
                    successMessageImpl(message, result);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("QueryCatalogController: handleGet -status: Exception occurred while fetching the query status:"
                              << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    } else {
        resourceNotFoundImpl(message);
    }
}

}// namespace NES
