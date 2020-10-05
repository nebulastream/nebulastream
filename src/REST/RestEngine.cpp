#include <Catalogs/StreamCatalog.hpp>
#include <REST/NetworkUtils.hpp>
#include <REST/RestEngine.hpp>
#include <Util/Logger.hpp>

#include "REST/Controller/BaseController.hpp"
#include "REST/Controller/QueryController.hpp"
#include "REST/Controller/StreamCatalogController.hpp"
#include <REST/Controller/ConnectivityController.hpp>
#include <REST/Controller/MonitoringController.hpp>
#include <REST/Controller/QueryCatalogController.hpp>
#include <REST/Controller/TopologyController.hpp>

namespace NES {

RestEngine::RestEngine(StreamCatalogPtr streamCatalog, NesCoordinatorWeakPtr coordinator, QueryCatalogPtr queryCatalog,
                       TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, QueryServicePtr queryService, MonitoringServicePtr monitoringService) : streamCatalog(streamCatalog) {
    NES_DEBUG("RestEngine");
    streamCatalogController = std::make_shared<StreamCatalogController>(streamCatalog);
    queryCatalogController = std::make_shared<QueryCatalogController>(queryCatalog, coordinator);
    queryController = std::make_shared<QueryController>(queryService, queryCatalog, topology, globalExecutionPlan);
    connectivityController = std::make_shared<ConnectivityController>();
    monitoringController = std::make_shared<MonitoringController>(monitoringService);
    topologyController = std::make_shared<TopologyController>(topology);
}

RestEngine::~RestEngine() {
    NES_DEBUG("~RestEngine()");
}

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
    NES_INFO("Defining endpoint using " + value);
    web::uri endpointURI(value);
    web::uri_builder endpointBuilder;

    endpointBuilder.set_scheme(endpointURI.scheme());
    endpointBuilder.set_host(endpointURI.host());

    endpointBuilder.set_port(endpointURI.port());
    endpointBuilder.set_path(endpointURI.path());

    _listener = web::http::experimental::listener::http_listener(endpointBuilder.to_uri());
}

void RestEngine::handleGet(http_request message) {
    auto path = getPath(message);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "query") {
            queryController->handleGet(paths, message);
            return;
        } else if (paths[0] == "streamCatalog") {
            streamCatalogController->handleGet(paths, message);
            return;
        } else if (paths[0] == "queryCatalog") {
            queryCatalogController->handleGet(paths, message);
            return;
        } else if (paths[0] == "monitoring") {
            monitoringController->handleGet(paths, message);
            return;
        } else if (paths[0] == "connectivity" && paths.size() == 1) {
            connectivityController->handleGet(paths, message);
        } else if (paths[0] == "topology") {
            topologyController->handleGet(paths, message);
        }
    }
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::GET, path));
}

void RestEngine::handlePost(http_request message) {
    auto path = getPath(message);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "query" || paths[0] == "pattern") {
            queryController->handlePost(paths, message);
            return;
        } else if (paths[0] == "streamCatalog") {
            streamCatalogController->handlePost(paths, message);
            return;
        }
        else if (paths[0] == "monitoring") {
            monitoringController->handlePost(paths, message);
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
            streamCatalogController->handleDelete(paths, message);
            return;
        } else if (paths[0] == "query") {
            queryController->handleDelete(paths, message);
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
    NES_DEBUG("RestEngine::shutdown(): Shutting Down Server");
    return _listener.close();
}

std::vector<utility::string_t> RestEngine::splitPath(const utility::string_t relativePath) {
    return web::uri::split_path(relativePath);
}

}// namespace NES