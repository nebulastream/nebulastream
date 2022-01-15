/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Catalogs/StreamCatalog.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <REST/Controller/BaseController.hpp>
#include <REST/Controller/ConnectivityController.hpp>
#include <REST/Controller/DumpFileController.hpp>
#include <REST/Controller/MonitoringController.hpp>
#include <REST/Controller/QueryCatalogController.hpp>
#include <REST/Controller/QueryController.hpp>
#include <REST/Controller/StreamCatalogController.hpp>
#include <REST/Controller/TopologyController.hpp>
#include <REST/Controller/UdfCatalogController.hpp>
#include <REST/RestEngine.hpp>
#include <Util/Logger.hpp>
#include <cpprest/details/http_server.h>
#include <cpprest/http_listener.h>
#include <iostream>

namespace NES {

RestEngine::RestEngine(const StreamCatalogPtr& streamCatalog,
                       const NesCoordinatorWeakPtr& coordinator,
                       const QueryCatalogPtr& queryCatalog,
                       const TopologyPtr& topology,
                       const GlobalExecutionPlanPtr& globalExecutionPlan,
                       const QueryServicePtr& queryService,
                       const MonitoringServicePtr& monitoringService,
                       const GlobalQueryPlanPtr& globalQueryPlan,
                       const Catalogs::UdfCatalogPtr& udfCatalog,
                       const Runtime::BufferManagerPtr bufferManager,
                       const WorkerRPCClientPtr workerClient) {
    streamCatalogController = std::make_shared<StreamCatalogController>(streamCatalog);
    queryCatalogController = std::make_shared<QueryCatalogController>(queryCatalog, coordinator, globalQueryPlan);
    queryController = std::make_shared<QueryController>(queryService, queryCatalog, topology, globalExecutionPlan);
    connectivityController = std::make_shared<ConnectivityController>();
    monitoringController = std::make_shared<MonitoringController>(monitoringService, bufferManager);
    topologyController = std::make_shared<TopologyController>(topology);
    udfCatalogController = std::make_shared<UdfCatalogController>(udfCatalog);
    dumpFileController = std::make_shared<DumpFileController>(topology, workerClient);
}

void RestEngine::initRestOpHandlers() {
    _listener.support(web::http::methods::GET, [this](auto&& PH1) {
        handleGet(std::forward<decltype(PH1)>(PH1));
    });
    _listener.support(web::http::methods::PUT, [this](auto&& PH1) {
        handlePut(std::forward<decltype(PH1)>(PH1));
    });
    _listener.support(web::http::methods::POST, [this](auto&& PH1) {
        handlePost(std::forward<decltype(PH1)>(PH1));
    });
    _listener.support(web::http::methods::DEL, [this](auto&& PH1) {
        handleDelete(std::forward<decltype(PH1)>(PH1));
    });
    _listener.support(web::http::methods::PATCH, [this](auto&& PH1) {
        handlePatch(std::forward<decltype(PH1)>(PH1));
    });
    _listener.support(web::http::methods::MERGE, [this](auto&& PH1) {
        handleMerge(std::forward<decltype(PH1)>(PH1));
    });
    _listener.support(web::http::methods::TRCE, [this](auto&& PH1) {
        handleTrace(std::forward<decltype(PH1)>(PH1));
    });
    _listener.support(web::http::methods::HEAD, [this](auto&& PH1) {
        handleHead(std::forward<decltype(PH1)>(PH1));
    });
    _listener.support(web::http::methods::OPTIONS, [this](auto&& PH1) {
        this->handlePreflightOptions(std::forward<decltype(PH1)>(PH1));
    });
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

void RestEngine::handleGet(web::http::http_request request) {
    auto const path = getPath(request);

    if (auto const paths = splitPath(path); !paths.empty()) {
        if (paths[0] == "query") {
            queryController->handleGet(paths, request);
            return;
        }
        if (paths[0] == "streamCatalog") {
            streamCatalogController->handleGet(paths, request);
            return;
        } else if (paths[0] == "queryCatalog") {
            queryCatalogController->handleGet(paths, request);
            return;
        } else if (paths[0] == "monitoring") {
            monitoringController->handleGet(paths, request);
            return;
        } else if (paths[0] == "connectivity" && paths.size() == 2) {
            connectivityController->handleGet(paths, request);
            return;
        } else if (paths[0] == "topology") {
            topologyController->handleGet(paths, request);
            return;
        } else if (paths[0] == UdfCatalogController::path_prefix) {
            udfCatalogController->handleGet(paths, request);
            return;
        } else if (paths[0] == "dumpFiles") {
            dumpFileController->handleGet(paths, request);
            return;
        }
    }
    returnDefaultUnknownEndpointResponse(request);
}

void RestEngine::handlePost(web::http::http_request request) {
    auto path = getPath(request);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "query") {
            queryController->handlePost(paths, request);
            return;
        }
        if (paths[0] == "streamCatalog") {
            streamCatalogController->handlePost(paths, request);
            return;
        } else if (paths[0] == "monitoring") {
            monitoringController->handlePost(paths, request);
            return;
        } else if (paths[0] == "topology") {
            topologyController->handlePost(paths, request);
            return;
        } else if (paths[0] == UdfCatalogController::path_prefix) {
            udfCatalogController->handlePost(paths, request);
            return;
        }
    }
    returnDefaultUnknownEndpointResponse(request);
}

void RestEngine::handleDelete(web::http::http_request request) {
    NES_DEBUG("RestEngine::handleDelete");
    auto path = getPath(request);
    auto paths = splitPath(path);

    if (!paths.empty()) {
        if (paths[0] == "streamCatalog") {
            streamCatalogController->handleDelete(paths, request);
            return;
        }
        if (paths[0] == "query") {
            queryController->handleDelete(paths, request);
            return;
        }
        if (paths[0] == UdfCatalogController::path_prefix) {
            udfCatalogController->handleDelete(paths, request);
            return;
        }
    }
    returnDefaultUnknownEndpointResponse(request);
}

utility::string_t RestEngine::getPath(web::http::http_request& request) {
    return web::uri::decode(request.relative_uri().path());
}

void RestEngine::handleHead(web::http::http_request request) {
    RestEngine::returnDefaultNotImplementedReply(web::http::methods::HEAD, request);
}

void RestEngine::handleMerge(web::http::http_request request) {
    RestEngine::returnDefaultNotImplementedReply(web::http::methods::MERGE, request);
}

void RestEngine::handleTrace(web::http::http_request request) {
    RestEngine::returnDefaultNotImplementedReply(web::http::methods::TRCE, request);
}

//TODO (covered in issue 1919 (Add option to configure whitelisted addresses for CORS))
// the '*' should be replaced at some point, with specifically allowed addresses, provided by a config
// Note: it is not possible to provide several allowed addresses at once, rather, a check should be performed here
void RestEngine::handlePreflightOptions(const web::http::http_request& request) {
    web::http::http_response response(web::http::status_codes::OK);
    response.headers().add(("Access-Control-Allow-Origin"), ("*"));
    response.headers().add(("Access-Control-Allow-Methods"), ("DELETE"));
    response.headers().add(("Access-Control-Allow-Headers"), ("Content-Type"));
    request.reply(response);
}

void RestEngine::handlePut(web::http::http_request request) {
    request.reply(web::http::status_codes::NotImplemented, responseNotImpl(web::http::methods::PUT, getPath(request)));
}

void RestEngine::handlePatch(web::http::http_request request) {
    request.reply(web::http::status_codes::NotImplemented, responseNotImpl(web::http::methods::PATCH, getPath(request)));
}

std::string RestEngine::endpoint() const { return _listener.uri().to_string(); }

pplx::task<void> RestEngine::accept() {
    initRestOpHandlers();
    return _listener.open();
}

pplx::task<void> RestEngine::shutdown() {
    NES_DEBUG("RestEngine::shutdown(): Shutting Down Server");
    return _listener.close();
}

std::vector<utility::string_t> RestEngine::splitPath(const utility::string_t& relativePath) {
    return web::uri::split_path(relativePath);
}

pplx::task<void> RestEngine::returnDefaultNotImplementedReply(const web::http::method& method, web::http::http_request& request) {
    return request.reply(web::http::status_codes::NotImplemented, responseNotImpl(method, getPath(request)));
}

web::json::value RestEngine::responseNotImpl(const web::http::method& method, utility::string_t path) {
    auto response = web::json::value::object();
    response["path"] = web::json::value::string(std::move(path));
    response["http_method"] = web::json::value::string(method);
    return response;
}

void RestEngine::returnDefaultUnknownEndpointResponse(web::http::http_request request) {
    web::json::value response{};
    response["detail"] = web::json::value::string("Unknown endpoint");
    BaseController::badRequestImpl(request, response);
}

}// namespace NES