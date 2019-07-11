
#include <Rest/std_service.hpp>
#include <Rest/svc_controller.hpp>
#include <API/InputQuery.hpp>
#include <CodeGen/QueryPlanBuilder.hpp>
#include <Rest/runtime_utils.hpp>
#include <Util/FogTopologyGenerator.hpp>

using namespace web;
using namespace http;
using namespace iotdb;

void ServiceController::initRestOpHandlers() {
    _listener.support(methods::GET, std::bind(&ServiceController::handleGet, this, std::placeholders::_1));
    _listener.support(methods::PUT, std::bind(&ServiceController::handlePut, this, std::placeholders::_1));
    _listener.support(methods::POST, std::bind(&ServiceController::handlePost, this, std::placeholders::_1));
    _listener.support(methods::DEL, std::bind(&ServiceController::handleDelete, this, std::placeholders::_1));
    _listener.support(methods::PATCH, std::bind(&ServiceController::handlePatch, this, std::placeholders::_1));
}

void ServiceController::handleGet(http_request message) {

    auto path = requestPath(message);
    if (!path.empty()) {
        if (path[0] == "service" && path[1] == "fog-plan") {

            FogTopologyGenerator fogTopologyGenerator;
            const std::shared_ptr<FogTopologyPlan> &exampleTopology = fogTopologyGenerator.generateExampleTopology();
            fogTopologyGenerator.getFogTopologyAsJson(&exampleTopology);

            http_response response(status_codes::OK);
            auto jsonResponse = json::value::object();
            jsonResponse["version"] = json::value::string("0.1.1.23232");
            jsonResponse["status"] = json::value::string("ready!");
            response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
            response.set_body(jsonResponse);
            message.reply(response);
        } else {
            http_response response(status_codes::NotFound);
            response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
            message.reply(response);
        }
    } else {
        http_response response(status_codes::NotFound);
        response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
        message.reply(response);
    }
}

void ServiceController::handlePatch(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PATCH));
}

void ServiceController::handlePut(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PUT));
}

void ServiceController::handlePost(http_request message) {
    try {

        auto path = requestPath(message);
        if (!path.empty()) {
            if (path[0] == "service" && path[1] == "query-plan") {

                message.extract_string(true)
                        .then([message](utility::string_t body) {
                                  try {
                                      //Prepare Input query from user string
                                      std::string userQuery(body.begin(), body.end());
                                      InputQuery inputQuery(iotdb::createQueryFromCodeString(userQuery));
                                      //Prepare response
                                      http_response response(status_codes::OK);
                                      QueryPlanBuilder queryPlanBuilder;
                                      const json::value &basePlan = queryPlanBuilder.getBasePlan(inputQuery);
                                      response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                                      response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                                      response.set_body(basePlan);
                                      message.reply(response);

                                  } catch (...) {
                                      std::cout << "Exception occurred while building the query plan for user request.";
                                      http_response response(status_codes::InternalError);
                                      response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                                      response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                                      message.reply(response);
                                  }
                              }
                        )
                        .wait();

            } else {
                http_response response(status_codes::NotFound);
                response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                message.reply(response);
            }
        } else {
            http_response response(status_codes::NotFound);
            response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
            message.reply(response);
        }
    } catch (const std::exception &ex) {
        std::cout << "Exception occurred during post request.";
    } catch (...) {
        RuntimeUtils::printStackTrace();
    }

}

void ServiceController::handleDelete(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL));
}

void ServiceController::handleHead(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD));
}

void ServiceController::handleOptions(http_request message) {
    http_response response(status_codes::OK);
    response.headers().add(U("Allow"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Methods"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    message.reply(response);
}

void ServiceController::handleTrace(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::TRCE));
}

void ServiceController::handleConnect(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::CONNECT));
}

void ServiceController::handleMerge(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE));
}

json::value ServiceController::responseNotImpl(const http::method &method) {
    auto response = json::value::object();
    response["serviceName"] = json::value::string("IotDB");
    response["http_method"] = json::value::string(method);
    return response;
}