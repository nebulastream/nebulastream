#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <REST/Controller/QueryController.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Topology/TestTopology.hpp>
#include <Util/Logger.hpp>

using namespace web;
using namespace http;
using namespace std;

namespace NES {

void QueryController::handleGet(vector<utility::string_t> path, http_request message) {

    if (path[1] == "nes-topology") {
        const auto& nesTopology = nesTopologyServicePtr->getNESTopologyAsJson();
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
                    string queryId = coordinatorServicePtr->registerQuery(userQuery, optimizationStrategyName);

                    NESExecutionPlanPtr executionPlan = coordinatorServicePtr->getRegisteredQuery(queryId);
                    json::value executionGraphPlan = executionPlan->getExecutionGraphAsJson();

                    json::value restResponse{};
                    restResponse["queryId"] = json::value::string(queryId);
                    restResponse["executionGraph"] = executionGraphPlan;
                    restResponse["planComputeTime"] =
                        json::value::string(std::to_string(executionPlan->getTotalComputeTimeInMillis()));

                    // Prepare the response
                    successMessageImpl(message, restResponse);
                    return;
                }
                catch (...) {
                    NES_DEBUG("Exception occurred while building the query plan for user request.");
                    internalServerErrorImpl(message);
                    return;
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
                            const auto& basePlan = queryServicePtr->generateBaseQueryPlanFromQueryString(
                                userQuery);

                            //Prepare the response
                            successMessageImpl(message, basePlan);
                            return;
                        } catch (...) {
                            NES_DEBUG("Exception occurred while building the query plan for user request.");
                            internalServerErrorImpl(message);
                            return;
                        }
                      }
                )
                .wait();
        }
    resourceNotFoundImpl(message);
}

void QueryController::handlePost(vector<utility::string_t> path, http_request message) {

    try {

         if (path[1] == "execute-query") {

            NES_DEBUG("Trying to execute query");

            message.extract_string(true)
                .then([this, message](utility::string_t body) {
                        try {
                            //Prepare Input query from user string
                            string userRequest(body.begin(), body.end());
                            NES_DEBUG("Request body: " << userRequest << "try to parse query");
                            json::value req = json::value::parse(userRequest);
                            NES_DEBUG( "get user query" );
                            string userQuery = req.at("userQuery").as_string();
                            NES_DEBUG( "query=" << userQuery );

                            NES_DEBUG( "try to parse strategy name" );
                            string optimizationStrategyName = req.at("strategyName").as_string();
                            NES_DEBUG( "strategyName=" << optimizationStrategyName );

                            NES_DEBUG( "Params: userQuery= " << userQuery << ", strategyName= "
                                      << optimizationStrategyName );

                            abstract_actor* abstractActor = caf::actor_cast<abstract_actor*>(coordinatorActorHandle);
                            CoordinatorActor* crd = dynamic_cast<CoordinatorActor*>(abstractActor);
                            string queryId = crd->registerAndDeployQuery(0, userQuery,
                                                                         optimizationStrategyName);

                            json::value restResponse{};
                            restResponse["queryId"] = json::value::string(queryId);

                            //Prepare the response
                            successMessageImpl(message, restResponse);
                            return;

                        } catch (...) {
                            NES_DEBUG("Exception occurred while building the query plan for user request.");
                            internalServerErrorImpl(message);
                            return;
                        }
                      }
                )
                .wait();
        }

        resourceNotFoundImpl(message);

    } catch (const std::exception& ex) {
        NES_DEBUG("Exception occurred during post request.");
        internalServerErrorImpl(message);
    } catch (...) {
        RuntimeUtils::printStackTrace();
        internalServerErrorImpl(message);
    }
}

void QueryController::setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {
    this->coordinatorActorHandle = coordinatorActorHandle;
}

}
