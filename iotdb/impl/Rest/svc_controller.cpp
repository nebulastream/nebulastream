
#include <Rest/std_service.hpp>
#include <Rest/svc_controller.hpp>
#include <API/InputQuery.hpp>

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
        if (path[0] == "service" && path[1] == "test") {

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

                message.extract_string(true).then(
                        [message](utility::string_t body) // this receieves the body of the message from our html page
                        {

                            std::string newbody(body.begin(),
                                                body.end()); //using this to convert from wstring to string, then

                        });

                http_response response(status_codes::OK);

                auto lvl2Node = json::value::object();
                lvl2Node["ID"] = json::value::string("3");
                lvl2Node["Name"] = json::value::string("Sink");

                std::vector<json::value>listOfLvl2Children;
                listOfLvl2Children.push_back(lvl2Node);

                auto lvl1Node = json::value::object();
                lvl1Node["ID"] = json::value::string("2");
                lvl1Node["Name"] = json::value::string("Map");
                lvl1Node["Child"] = json::value::array(listOfLvl2Children);

                std::vector<json::value>listOfLvl1Children;
                listOfLvl1Children.push_back(lvl1Node);

                auto rootNode = json::value::object();
                rootNode["ID"] = json::value::string("1");
                rootNode["Name"] = json::value::string("Source");
                rootNode["Child"] = json::value::array(listOfLvl1Children);
                response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                response.set_body(rootNode);
                message.reply(response);

//            InputQuery q(iotdb::createQueryFromCodeString(std::to_string(message.body())));
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
        std::cout << ex.what();
    } catch (const std::string &ex) {
        std::cout << ex;
        // ...
    } catch (...) {
        std::cout << "Exception occured";
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