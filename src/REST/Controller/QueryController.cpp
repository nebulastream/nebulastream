#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <REST/Controller/QueryController.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

using namespace web;
using namespace http;
using namespace std;

namespace NES {

QueryController::QueryController(QueryServicePtr queryService, QueryCatalogPtr queryCatalog, TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan)
    : queryService(queryService), queryCatalog(queryCatalog), topology(topology), globalExecutionPlan(globalExecutionPlan) {}

void QueryController::handleGet(vector<utility::string_t> path, http_request message) {

    if (path[1] == "execution-plan") {
        // There will be change in the structure of globalExecutionPlan, so we will wait for that to be resolve
        // then create the execution-plan based on the new structure
        message.reply(status_codes::NotImplemented, responseNotImpl(methods::GET, path[1]));
    } else if (path[1] == "query-plan") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    string userRequest(body.begin(), body.end());

                    json::value req = json::value::parse(userRequest);

                    QueryId queryId = req.at("userQuery").as_integer();

                    //Call the service
                    auto basePlan = UtilityFunctions::getQueryPlanAsJson(queryCatalog, queryId);

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

        if (path[1] == "execute-query"){
            NES_DEBUG(" QueryController: Trying to execute query");
        }
        else if (path[1] == "execute-pattern"){
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
                    if (req.has_field("userQuery")){
                        userQuery = req.at("userQuery").as_string();
                    }
                    else if (req.has_field("pattern")){
                        userQuery = req.at("pattern").as_string();
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

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    std::string payload(body.begin(), body.end());
                    json::value req = json::value::parse(payload);
                    QueryId queryId = req.at("queryId").as_integer();

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
