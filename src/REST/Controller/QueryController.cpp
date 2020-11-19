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
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Utils/PlanJsonGenerator.hpp>
#include <REST/Controller/QueryController.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <Util/Logger.hpp>

using namespace web;
using namespace http;
using namespace std;

namespace NES {

QueryController::QueryController(QueryServicePtr queryService, QueryCatalogPtr queryCatalog, TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan)
    : queryService(queryService), queryCatalog(queryCatalog), topology(topology), globalExecutionPlan(globalExecutionPlan) {}

void QueryController::handleGet(vector<utility::string_t> path, http_request message) {

    if (path[1] == "execution-plan") {
        NES_INFO("QueryController:: execution-plan");
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    // get the queryId from user input
                    string userRequest(body.begin(), body.end());
                    json::value req = json::value::parse(userRequest);
                    QueryId queryId = req.at("queryId").as_integer();

                    NES_DEBUG("QueryController:: execution-plan requested queryId: " << queryId);
                    // get the execution-plan for given query id
                    auto executionPlanJson = PlanJsonGenerator::getExecutionPlanAsJson(globalExecutionPlan, queryId);
                    NES_DEBUG("QueryController:: execution-plan: " << executionPlanJson.serialize());
                    //Prepare the response
                    successMessageImpl(message, executionPlanJson);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            });
    } else if (path[1] == "query-plan") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    string userRequest(body.begin(), body.end());

                    json::value req = json::value::parse(userRequest);

                    QueryId queryId = req.at("userQuery").as_integer();

                    //Call the service
                    NES_DEBUG("UtilityFunctions: Get the registered query");
                    if (!queryCatalog->queryExists(queryId)) {
                        throw QueryNotFoundException("QueryService: Unable to find query with id " + std::to_string(queryId) + " in query catalog.");
                    }
                    QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);

                    NES_DEBUG("UtilityFunctions: Getting the json representation of the query plan");
                    auto basePlan = PlanJsonGenerator::getQueryPlanAsJson(queryCatalogEntry->getQueryPlan());

                    //Prepare the response
                    successMessageImpl(message, basePlan);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("QueryController: handleGet -query-plan: Exception occurred while building the query plan for user request:"
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

void QueryController::handlePost(vector<utility::string_t> path, http_request message) {

    if (path[1] == "execute-query" || path[1] == "execute-pattern") {

        if (path[1] == "execute-query") {
            NES_DEBUG(" QueryController: Trying to execute query");
        } else if (path[1] == "execute-pattern") {
            NES_DEBUG(" QueryController: Trying to execute pattern as stream query");
        }

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    string userRequest(body.begin(), body.end());
                    NES_DEBUG("QueryController: handlePost -execute-query: Request body: " << userRequest << "try to parse query");
                    json::value req = json::value::parse(userRequest);
                    NES_DEBUG("QueryController: handlePost -execute-query: get user query");
                    string userQuery = "";
                    if (req.has_field("userQuery")) {
                        userQuery = req.at("userQuery").as_string();
                    } else if (req.has_field("pattern")) {
                        userQuery = req.at("pattern").as_string();
                    } else {
                        NES_ERROR("QueryController: handlePost -execute-query: Wrong key word for user query or pattern. Use either 'userQuery' or 'pattern'.");
                    }

                    string optimizationStrategyName = req.at("strategyName").as_string();
                    NES_DEBUG("QueryController: handlePost -execute-query: Params: userQuery= " << userQuery << ", strategyName= "
                                                                                                << optimizationStrategyName);
                    QueryId queryId = queryService->validateAndQueueAddRequest(userQuery, optimizationStrategyName);

                    //Prepare the response
                    json::value restResponse{};
                    restResponse["queryId"] = json::value::number(queryId);
                    successMessageImpl(message, restResponse);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR("QueryController: handlePost -execute-query: Exception occurred while building the query plan for user request:" << exc.what());
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

void QueryController::handleDelete(std::vector<utility::string_t> path, http_request message) {
    if (path[1] == "stop-query") {
        NES_DEBUG("stop-query msg=" << message.to_string());
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    json::value req = json::value::parse(payload);
                    cout << "req=" << req.as_integer() << endl;
                    QueryId queryId = req.as_integer();

                    bool success = queryService->validateAndQueueStopRequest(queryId);
                    //Prepare the response
                    json::value result{};
                    result["success"] = json::value::boolean(success);
                    successMessageImpl(message, result);
                    return;
                } catch (QueryNotFoundException& exc) {
                    NES_ERROR("QueryCatalogController: handleDelete -query: Exception occurred while building the query plan for user request:"
                              << exc.what());
                    handleException(message, exc);
                    return;
                } catch (InvalidQueryStatusException& exc) {
                    NES_ERROR("QueryCatalogController: handleDelete -query: Exception occurred while building the query plan for user request:"
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
