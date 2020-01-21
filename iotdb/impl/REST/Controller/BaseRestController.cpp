#include "REST/Controller/BaseRestController.hpp"
#include "REST/NetworkUtils.hpp"

namespace NES {

void BaseRestController::initRestOpHandlers() {
    _listener.support(methods::GET, std::bind(&BaseRestController::handleGet, this, std::placeholders::_1));
    _listener.support(methods::PUT, std::bind(&BaseRestController::handlePut, this, std::placeholders::_1));
    _listener.support(methods::POST, std::bind(&BaseRestController::handlePost, this, std::placeholders::_1));
    _listener.support(methods::DEL, std::bind(&BaseRestController::handleDelete, this, std::placeholders::_1));
    _listener.support(methods::PATCH, std::bind(&BaseRestController::handlePatch, this, std::placeholders::_1));
}

void BaseRestController::setEndpoint(const std::string& value) {
    std::cout << "Defining endpoint using " + value << std::endl;
    web::uri endpointURI(value);
    web::uri_builder endpointBuilder;

    endpointBuilder.set_scheme(endpointURI.scheme());
    endpointBuilder.set_host(endpointURI.host());

    endpointBuilder.set_port(endpointURI.port());
    endpointBuilder.set_path(endpointURI.path());

    _listener = http_listener(endpointBuilder.to_uri());
}

//NOTE: maybe someone can suggest a better way to do this.
void BaseRestController::setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle) {
    this->queryController.setCoordinatorActorHandle(coordinatorActorHandle);
};

void BaseRestController::handleGet(http_request message) {

    auto path = getPath(message);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "query") {
            queryController.handleGet(paths, message);
            return;
        } else if (paths[0] == "streamCatalog") {
            streamCatalogController.handleGet(paths, message);
            return;
        }
    }
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::GET, path));
}

void BaseRestController::handlePost(http_request message) {
    auto path = getPath(message);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "query") {
            queryController.handlePost(paths, message);
            return;
        } else if (paths[0] == "streamCatalog") {
            streamCatalogController.handlePost(paths, message);
            return;
        }
    }
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::POST, path));
}

void BaseRestController::handlePatch(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PATCH, getPath(message)));
}

void BaseRestController::handlePut(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PUT, getPath(message)));
}

void BaseRestController::handleDelete(http_request message) {
    auto path = getPath(message);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "catalog") {
            streamCatalogController.handleDelete(paths, message);
            return;
        }
    }
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL, path));
}

void BaseRestController::handleHead(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD, getPath(message)));
}

void BaseRestController::handleOptions(http_request message) {
    http_response response(status_codes::OK);
    response.headers().add(U("Allow"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Methods"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    message.reply(response);
}

void BaseRestController::handleTrace(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::TRCE, getPath(message)));
}

void BaseRestController::handleConnect(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::CONNECT, getPath(message)));
}

void BaseRestController::handleMerge(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE, getPath(message)));
}

json::value BaseRestController::responseNotImpl(const http::method& method, utility::string_t path) {
    auto response = json::value::object();
    response["path"] = json::value::string(path);
    response["http_method"] = json::value::string(method);
    return response;
}

std::string BaseRestController::endpoint() const {
    return _listener.uri().to_string();
}

pplx::task<void> BaseRestController::accept() {
    initRestOpHandlers();
    return _listener.open();
}

pplx::task<void> BaseRestController::shutdown() {
    return _listener.close();
}

std::vector<utility::string_t> BaseRestController::splitPath(const utility::string_t relativePath) {
    return web::uri::split_path(relativePath);
}

utility::string_t BaseRestController::getPath(http_request& message) {
    return web::uri::decode(message.relative_uri().path());
}

}