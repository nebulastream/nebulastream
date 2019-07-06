
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

                            std::stringstream code;
                            code << "Config config = Config::create()"
                                    ".setBufferCount(2000)"
                                    ".setBufferSizeInByte(8*1024)"
                                    ".setNumberOfWorker(2);"
                                 << std::endl;

                            code << "Schema schema = Schema::create().addField(\"\",INT32);" << std::endl;

                            code << "DataSourcePtr source = createTestSource();" << std::endl;

                            code << "return InputQuery::create(config, source)" << std::endl
                                 << ".filter(PredicatePtr())" << std::endl
                                 << ".printInputQueryPlan();" << std::endl;

                            InputQuery q(iotdb::createQueryFromCodeString(code.str()));
                            q.printInputQueryPlan();
                        });

                http_response response(status_codes::OK);

                auto lvl2Node = json::value::object();
                lvl2Node["name"] = json::value::string("Sink-3");

                std::vector<json::value> listOfLvl2Children;
                listOfLvl2Children.push_back(lvl2Node);

                auto lvl1Node = json::value::object();
                lvl1Node["name"] = json::value::string("Map-1");
                lvl1Node["children"] = json::value::array(listOfLvl2Children);

                std::vector<json::value> listOfLvl1Children;
                listOfLvl1Children.push_back(lvl1Node);

                auto rootNode = json::value::object();
                rootNode["name"] = json::value::string("Source-0");
                rootNode["children"] = json::value::array(listOfLvl1Children);
                response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
                response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
                response.set_body(rootNode);
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