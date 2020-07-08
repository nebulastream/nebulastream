#include <Catalogs/QueryCatalog.hpp>
#include <Components/NesCoordinator.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <REST/Controller/QueryController.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>

using namespace web;
using namespace http;

namespace NES {

QueryController::QueryController(QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager, StreamCatalogPtr streamCatalog,
                                 GlobalExecutionPlanPtr globalExecutionPlan) : queryCatalog(queryCatalog), topologyManager(topologyManager),
                                                                               streamCatalog(streamCatalog), globalExecutionPlan(globalExecutionPlan) {
    queryServicePtr = std::make_shared<QueryService>(queryCatalog);
}

void QueryController::handleGet(vector<utility::string_t> path, http_request message) {

    if (path[1] == "nes-topology") {
        const auto& nesTopology = topologyManager->getNESTopologyGraphAsJson();
        //Prepare the response
        successMessageImpl(message, nesTopology);
        return;
    } else if (path[1] == "execution-plan") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    // Prepare Input query from user string
                    string userRequest(body.begin(), body.end());
                    json::value req = json::value::parse(userRequest);
                    string userQuery = req.at("userQuery").as_string();
                    string optimizationStrategyName = req.at("strategyName").as_string();

                    // Call the service
                    string queryId = queryCatalog->registerAndAddToSchedulingQueue(userQuery, optimizationStrategyName);
                    std::string executionPlanAsString = globalExecutionPlan->getAsString();

                    // Prepare the response
                    json::value restResponse{};
                    restResponse["queryId"] = json::value::string(queryId);
                    restResponse["executionPlan"] = json::value::string(executionPlanAsString);
                    successMessageImpl(message, restResponse);
                    return;
                } catch (const std::exception& exc) {
                    NES_ERROR(" QueryController: handleGet -execution-plan: Exception occurred while building the query plan for user request:" << exc.what());
                    handleException(message, exc);
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                }
            })
            .wait();
    } else if (path[1] == "query-plan") {
        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    string userRequest(body.begin(), body.end());

                    json::value req = json::value::parse(userRequest);

                    string userQuery = req.at("userQuery").as_string();

                    //Call the service
                    auto basePlan = queryServicePtr->getQueryPlanForQueryId(userQuery);

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

    if (path[1] == "execute-query") {

        NES_DEBUG(" QueryController: Trying to execute query");

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    //Prepare Input query from user string
                    string userRequest(body.begin(), body.end());
                    NES_DEBUG("QueryController: handlePost -execute-query: Request body: " << userRequest << "try to parse query");
                    json::value req = json::value::parse(userRequest);
                    NES_DEBUG("QueryController: handlePost -execute-query: get user query");
                    string userQuery = req.at("userQuery").as_string();
                    string optimizationStrategyName = req.at("strategyName").as_string();
                    NES_DEBUG("QueryController: handlePost -execute-query: Params: userQuery= " << userQuery << ", strategyName= "
                                                                                                << optimizationStrategyName);
                    string queryId = queryServicePtr->validateAndRegisterQuery(userQuery, optimizationStrategyName);

                    //                    string queryId = coordinator->addQuery(userQuery, optimizationStrategyName);

                    //Prepare the response
                    json::value restResponse{};
                    restResponse["queryId"] = json::value::string(queryId);
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

}// namespace NES
