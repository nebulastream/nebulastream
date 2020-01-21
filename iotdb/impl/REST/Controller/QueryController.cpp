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

        createExampleTopology();
        const auto& nesTopology = nesTopologyService.getNESTopologyAsJson();

        http_response response(status_codes::OK);
        response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
        response.set_body(nesTopology);
        message.reply(response);
        return;
    }

    http_response response(status_codes::NotFound);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    message.reply(response);
}

void QueryController::handlePost(vector<utility::string_t> path, http_request message) {

    try {

        if (path[1] == "query-plan") {

            message.extract_string(true)
                .then([this, message](utility::string_t body) {
                        try {
                            //Prepare Input query from user string
                            std::string userQuery(body.begin(), body.end());

                            //Call the service
                            const auto& basePlan = queryService.generateBaseQueryPlanFromQueryString(
                                userQuery);

                            //Prepare the response
                            http_response response(status_codes::OK);
                            response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                            response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                            response.set_body(basePlan);
                            message.reply(response);
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
                            const string
                                queryId = coordinatorServicePtr->register_query(userQuery, optimizationStrategyName);
                            NESExecutionPlan* executionPlan = coordinatorServicePtr->getRegisteredQuery(queryId);

                            json::value executionGraphPlan = executionPlan->getExecutionGraphAsJson();

                            //Prepare the response
                            http_response response(status_codes::OK);
                            response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                            response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                            response.set_body(executionGraphPlan);
                            message.reply(response);
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

                            json::value req = json::value::parse(userRequest);

                            string userQuery = req.at("userQuery").as_string();
                            string optimizationStrategyName = req.at("strategyName").as_string();

                            std::cout << "Params: userQuery= " << userQuery << ", strategyName= "
                                      << optimizationStrategyName << std::endl;

                            //Perform async call for deploying the query using actor
                            //Note: This is an async call and would not know if the deployment has failed
                            CoordinatorActorConfig actorCoordinatorConfig;
                            actorCoordinatorConfig.load<io::middleman>();
                            //Prepare Actor System
                            actor_system actorSystem{actorCoordinatorConfig};
                            scoped_actor self{actorSystem};

                            string queryId;
                            self->request(coordinatorActorHandle,
                                          task_timeout,
                                          execute_query_atom::value,
                                          userQuery,
                                          optimizationStrategyName).receive(
                                [&queryId](const string& _uuid) mutable {
                                  queryId = _uuid;
                                },
                                [=](const error& er) {
                                  string error_msg = to_string(er);
                                });

                            json::value result{};
                            result["queryId"] = json::value::string(queryId);

                            //Prepare the response
                            http_response response(status_codes::OK);
                            response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                            response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                            response.set_body(result);
                            message.reply(response);
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

        http_response response(status_codes::NotFound);
        response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
        message.reply(response);

    } catch (const std::exception& ex) {
        std::cout << "Exception occurred during post request.";
        internalServerErrorImpl(message);
    } catch (...) {
        RuntimeUtils::printStackTrace();
        internalServerErrorImpl(message);
    }
}

void QueryController::internalServerErrorImpl(http_request message) {
    http_response response(status_codes::InternalError);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    message.reply(response);
}

void QueryController::setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {
    this->coordinatorActorHandle = coordinatorActorHandle;
}

}


