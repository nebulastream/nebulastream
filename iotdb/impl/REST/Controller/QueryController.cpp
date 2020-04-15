#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <REST/Controller/QueryController.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Topology/TestTopology.hpp>

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
    }
    resourceNotFoundImpl(message);
}

void QueryController::handlePost(vector<utility::string_t> path, http_request message) {

    try {

        if (path[1] == "query-plan") {

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
                            std::cout << "Exception occurred while building the query plan for user request.";
                            internalServerErrorImpl(message);
                            return;
                        }
                      }
                )
                .wait();
        } else if (path[1] == "execution-plan") {
            message.extract_string(true)
                .then([this, message](utility::string_t body) {
                        try {
                            //Prepare Input query from user string
                            string userRequest(body.begin(), body.end());

                            json::value req = json::value::parse(userRequest);

                            string userQuery = req.at("userQuery").as_string();
                            string optimizationStrategyName = req.at("strategyName").as_string();

                            //FIXME: setup example topology
                            //TODO: do we really have to do this for each submitted query? Cannot we solve it somehow else
                            createExampleTopology();

                            //Call the service
                            string queryId = coordinatorServicePtr->registerQuery(userQuery, optimizationStrategyName);

                            NESExecutionPlanPtr executionPlan = coordinatorServicePtr->getRegisteredQuery(queryId);
                            json::value executionGraphPlan = executionPlan->getExecutionGraphAsJson();

                            json::value restResponse{};
                            restResponse["queryId"] = json::value::string(queryId);
                            restResponse["executionGraph"] = executionGraphPlan;
                            restResponse["planComputeTime"] =
                                json::value::string(std::to_string(executionPlan->getTotalComputeTimeInMillis()));

                            //Prepare the response
                            successMessageImpl(message, restResponse);
                            return;
                        } catch (...) {
                            std::cout << "Exception occurred while building the query plan for user request.";
                            internalServerErrorImpl(message);
                            return;
                        }
                      }
                )
                .wait();

        } else if (path[1] == "execute-query") {

            std::cout << "Trying to execute query: -> " << std::endl;

            message.extract_string(true)
                .then([this, message](utility::string_t body) {
                        try {
                            //Prepare Input query from user string
                            string userRequest(body.begin(), body.end());
                            std::cout << "Request body: " << userRequest << std::endl;
                            std::cout << "try to parse query" << std::endl;
                            json::value req = json::value::parse(userRequest);
                            std::cout << "get user query" << std::endl;
                            string userQuery = req.at("userQuery").as_string();
                            std::cout << "query=" << userQuery << std::endl;

                            std::cout << "try to parse strategy name" << std::endl;
                            string optimizationStrategyName = req.at("strategyName").as_string();
                            std::cout << "strategyName=" << optimizationStrategyName << std::endl;

                            std::cout << "Params: userQuery= " << userQuery << ", strategyName= "
                                      << optimizationStrategyName << std::endl;

                            abstract_actor* abstractActor = caf::actor_cast<abstract_actor*>(coordinatorActorHandle);
                            CoordinatorActor* crd = dynamic_cast<CoordinatorActor*>(abstractActor);
                            string queryId = crd->executeQuery(userQuery,
                                                               optimizationStrategyName);

                            json::value restResponse{};
                            restResponse["queryId"] = json::value::string(queryId);

                            //Prepare the response
                            successMessageImpl(message, restResponse);
                            return;

                        } catch (...) {
                            std::cout << "Exception occurred while building the query plan for user request.";
                            internalServerErrorImpl(message);
                            return;
                        }
                      }
                )
                .wait();
        }

        resourceNotFoundImpl(message);

    } catch (const std::exception& ex) {
        std::cout << "Exception occurred during post request.";
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
