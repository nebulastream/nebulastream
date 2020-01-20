#include "REST/Controller/StreamCatalogController.hpp"

namespace NES {

void StreamCatalogController::initRestOpHandlers() {
    _listener.support(methods::GET, std::bind(&StreamCatalogController::handleGet, this, std::placeholders::_1));
    _listener.support(methods::PUT, std::bind(&StreamCatalogController::handlePut, this, std::placeholders::_1));
    _listener.support(methods::POST, std::bind(&StreamCatalogController::handlePost, this, std::placeholders::_1));
    _listener.support(methods::DEL, std::bind(&StreamCatalogController::handleDelete, this, std::placeholders::_1));
    _listener.support(methods::PATCH, std::bind(&StreamCatalogController::handlePatch, this, std::placeholders::_1));
}

void StreamCatalogController::setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {
    this->coordinatorActorHandle = coordinatorActorHandle;
};

void StreamCatalogController::handleGet(http_request message) {

    auto path = requestPath(message);
    if (!path.empty()) {
        if (path[0] == "service" && path[1] == "random") {

            http_response response(status_codes::OK);
            response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
            response.set_body("lalala");
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

void StreamCatalogController::handlePatch(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PATCH));
}

void StreamCatalogController::handlePut(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PUT));
}

void StreamCatalogController::handlePost(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::POST));
}

void StreamCatalogController::handleDelete(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL));
}

void StreamCatalogController::handleHead(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD));
}

void StreamCatalogController::handleOptions(http_request message) {
    http_response response(status_codes::OK);
    response.headers().add(U("Allow"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Methods"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    message.reply(response);
}

void StreamCatalogController::handleTrace(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::TRCE));
}

void StreamCatalogController::handleConnect(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::CONNECT));
}

void StreamCatalogController::handleMerge(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE));
}

json::value StreamCatalogController::responseNotImpl(const http::method &method) {
    auto response = json::value::object();
    response["serviceName"] = json::value::string("NES");
    response["http_method"] = json::value::string(method);
    return response;
}

}


