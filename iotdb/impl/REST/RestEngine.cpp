#include "REST/RestEngine.hpp"
#include "REST/NetworkUtils.hpp"

namespace NES {

void RestEngine::initRestOpHandlers() {
    _listener.support(methods::GET, std::bind(&RestEngine::handleGet, this, std::placeholders::_1));
    _listener.support(methods::PUT, std::bind(&RestEngine::handlePut, this, std::placeholders::_1));
    _listener.support(methods::POST, std::bind(&RestEngine::handlePost, this, std::placeholders::_1));
    _listener.support(methods::DEL, std::bind(&RestEngine::handleDelete, this, std::placeholders::_1));
    _listener.support(methods::PATCH, std::bind(&RestEngine::handlePatch, this, std::placeholders::_1));
    _listener.support(methods::MERGE, std::bind(&RestEngine::handleMerge, this, std::placeholders::_1));
    _listener.support(methods::TRCE, std::bind(&RestEngine::handleTrace, this, std::placeholders::_1));
    _listener.support(methods::HEAD, std::bind(&RestEngine::handleHead, this, std::placeholders::_1));
}

void RestEngine::setEndpoint(const std::string& value) {
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
void RestEngine::setCoordinator(NesCoordinatorPtr coordinator) {
    this->queryController.setCoordinator(coordinator);
    this->queryCatalogController.setCoordinator(coordinator);
};

void RestEngine::handleGet(http_request message) {

    auto path = getPath(message);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "query") {
            queryController.handleGet(paths, message);
            return;
        } else if (paths[0] == "streamCatalog") {
            streamCatalogController.handleGet(paths, message);
            return;
        } else if (paths[0] == "queryCatalog") {
            queryCatalogController.handleGet(paths, message);
            return;
        }
    }
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::GET, path));
}

void RestEngine::handlePost(http_request message) {
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

void RestEngine::handleDelete(http_request message) {
    auto path = getPath(message);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "streamCatalog") {
            streamCatalogController.handleDelete(paths, message);
            return;
        } else if (paths[0] == "queryCatalog") {
            queryCatalogController.handleDelete(paths, message);
            return;
        }
    }
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL, path));
}

void RestEngine::handleHead(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD, getPath(message)));
}

void RestEngine::handleMerge(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE, getPath(message)));
}

void RestEngine::handleTrace(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::TRCE, getPath(message)));
}

void RestEngine::handlePut(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PUT, getPath(message)));
}

void RestEngine::handlePatch(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PATCH, getPath(message)));
}

std::string RestEngine::endpoint() const {
    return _listener.uri().to_string();
}

pplx::task<void> RestEngine::accept() {
    initRestOpHandlers();
    return _listener.open();
}

pplx::task<void> RestEngine::shutdown() {
    cout << "Shutting Down Server" << endl;
    return _listener.close();
}

std::vector<utility::string_t> RestEngine::splitPath(const utility::string_t relativePath) {
    return web::uri::split_path(relativePath);
}

}// namespace NES