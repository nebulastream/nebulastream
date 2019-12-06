#include <API/InputQuery.hpp>

#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <REST/Controller/RestController.hpp>
#include <CodeGen/QueryPlanBuilder.hpp>
#include <Topology/FogTopologyManager.hpp>

using namespace web;
using namespace http;
using namespace iotdb;

void RestController::initRestOpHandlers() {
  _listener.support(methods::GET, std::bind(&RestController::handleGet, this, std::placeholders::_1));
  _listener.support(methods::PUT, std::bind(&RestController::handlePut, this, std::placeholders::_1));
  _listener.support(methods::POST, std::bind(&RestController::handlePost, this, std::placeholders::_1));
  _listener.support(methods::DEL, std::bind(&RestController::handleDelete, this, std::placeholders::_1));
  _listener.support(methods::PATCH, std::bind(&RestController::handlePatch, this, std::placeholders::_1));
}

void RestController::handleGet(http_request message) {

  auto path = requestPath(message);
  if (!path.empty()) {
    if (path[0] == "service" && path[1] == "fog-plan") {

      const auto &fogTopology = fogTopologyService.getFogTopologyAsJson();

      http_response response(status_codes::OK);
      response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
      response.set_body(fogTopology);
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

void RestController::handlePatch(http_request message) {
  message.reply(status_codes::NotImplemented, responseNotImpl(methods::PATCH));
}

void RestController::handlePut(http_request message) {
  message.reply(status_codes::NotImplemented, responseNotImpl(methods::PUT));
}

void RestController::handlePost(http_request message) {
  try {

    auto path = requestPath(message);
    if (!path.empty()) {
      if (path[0] == "service" && path[1] == "query-plan") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                      //Prepare Input query from user string
                      std::string userQuery(body.begin(), body.end());

                      //Call the service
                      const auto &basePlan = queryService.generateBaseQueryPlanFromQueryString(
                          userQuery);

                      //Prepare the response
                      http_response response(status_codes::OK);
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

      } else if (path[0] == "service" && path[1] == "execution-plan") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                      //Prepare Input query from user string
                      string userRequest(body.begin(), body.end());

                      json::value req = json::value::parse(userRequest);

                      string userQuery = req.at("userQuery").as_string();
                      string optimizationStrategyName = req.at("strategyName").as_string();

                      //Call the service
                      const string queryId = coordinatorService->register_query(userQuery, optimizationStrategyName);
                      FogExecutionPlan *executionPlan = coordinatorService->getRegisteredQuery(queryId);

                      json::value executionGraphPlan = executionPlan->getExecutionGraphAsJson();

                      //Prepare the response
                      http_response response(status_codes::OK);
                      response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                      response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                      response.set_body(executionGraphPlan);
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

      } else if (path[0] == "service" && path[1] == "execute-query") {

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                    try {
                      //Prepare Input query from user string
                      string userRequest(body.begin(), body.end());

                      json::value req = json::value::parse(userRequest);

                      string userQuery = req.at("userQuery").as_string();
                      string optimizationStrategyName = req.at("strategyName").as_string();

                      //Call the service
                      const string queryId = coordinatorService->executeQuery(userQuery, optimizationStrategyName);

                      //Prepare the response
                      http_response response(status_codes::OK);
                      response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                      response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                      response.set_body(queryId);
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

void RestController::handleDelete(http_request message) {
  message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL));
}

void RestController::handleHead(http_request message) {
  message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD));
}

void RestController::handleOptions(http_request message) {
  http_response response(status_codes::OK);
  response.headers().add(U("Allow"), U("GET, POST, OPTIONS"));
  response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
  response.headers().add(U("Access-Control-Allow-Methods"), U("GET, POST, OPTIONS"));
  response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
  message.reply(response);
}

void RestController::handleTrace(http_request message) {
  message.reply(status_codes::NotImplemented, responseNotImpl(methods::TRCE));
}

void RestController::handleConnect(http_request message) {
  message.reply(status_codes::NotImplemented, responseNotImpl(methods::CONNECT));
}

void RestController::handleMerge(http_request message) {
  message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE));
}

json::value RestController::responseNotImpl(const http::method &method) {
  auto response = json::value::object();
  response["serviceName"] = json::value::string("IotDB");
  response["http_method"] = json::value::string(method);
  return response;
}