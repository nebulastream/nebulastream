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

#include <Mobility/LocationService.h>
#include <Mobility/Utils/GeoNodeUtils.h>
#include <REST/Controller/LocationController.hpp>
#include <REST/runtime_utils.hpp>
#include <Util/Logger.hpp>

namespace NES {

LocationController::LocationController(std::shared_ptr<NES::LocationService> locationService)
    : locationService(std::move(locationService)) {
    NES_DEBUG("LocationController: Initializing");
}

void LocationController::handleGet(const std::vector<utility::string_t>& paths, const http_request& message) {
    try {
        if (paths.size() == 2 && paths[1] == "nodes") {
            handleGetLocations(message);
            return;
        }

        if (paths.size() == 2 && (paths[1] == "sink" || paths[1] == "source" )) {
            handleGetNode(message, paths[1]);
            return;
        }

        resourceNotFoundImpl(message);
    } catch (const std::exception& exc) {
        NES_ERROR("LocationController: Exception occurred during handlePost: " << exc.what());
        message.reply(status_codes::BadRequest, exc.what());
        return;
    } catch (...) {
        RuntimeUtils::printStackTrace();
        internalServerErrorImpl(message);
        return;
    }
}

void LocationController::handlePost(std::vector<utility::string_t> path, web::http::http_request message) {
    try {

        if (path.size() == 2 && path[1] == "location") {
            handleUpdateLocation(message);
            return;
        }

        if (path.size() == 2 && (path[1] == "sink" || path[1] == "source" )) {
            handleAddNode(message, path[1]);
            return;
        }

        resourceNotFoundImpl(message);
    } catch (const std::exception& exc) {
        NES_ERROR("LocationController: Exception occurred during handlePost: " << exc.what());
        message.reply(status_codes::BadRequest, exc.what());
        return;
    } catch (...) {
        RuntimeUtils::printStackTrace();
        internalServerErrorImpl(message);
        return;
    }
}

void LocationController::handleGetLocations(web::http::http_request message) {
    NES_DEBUG("LocationController: Getting information about all nodes");
    std::vector<web::json::value> sinks = {};

    for (auto const& [nodeId, sink] : locationService->getLocationCatalog()->getSinks()) {
        web::json::value currentNodeJsonValue = GeoNodeUtils::generateJson(sink);
        sinks.push_back(currentNodeJsonValue);
    }

    std::vector<web::json::value> sources = {};
    for (auto const& [nodeId, source] : locationService->getLocationCatalog()->getSources()) {
        web::json::value currentNodeJsonValue = GeoNodeUtils::generateJson(source);
        sources.push_back(currentNodeJsonValue);
    }

    web::json::value responseJson{};
    responseJson["sinks"] = web::json::value::array(sinks);
    responseJson["sources"] = web::json::value::array(sources);

    successMessageImpl(message, responseJson);
}

void LocationController::handleGetNode(web::http::http_request message, const std::string& path) {
    NES_DEBUG("LocationController: GET " << path);
    auto parameters = getParameters(message);

    const auto requestParameter = parameters.find("nodeId");
    if (requestParameter == parameters.end()) {
        NES_ERROR("LocationController: Unable to find node ID for the GET " << path << " request");
        json::value errorResponse{};
        errorResponse["detail"] = json::value::string("Parameter nodeId must be provided");
        badRequestImpl(message, errorResponse);
        return;
    }

    std::string nodeId = requestParameter->second;

    if (locationService->getLocationCatalog()->contains(nodeId)) {

        web::json::value nodeJsonValue;
        if (path == "sink") {
            GeoSinkPtr sink = locationService->getLocationCatalog()->getSinks().at(nodeId);
            nodeJsonValue = GeoNodeUtils::generateJson(sink);
        }

        if (path == "source") {
            GeoSourcePtr source = locationService->getLocationCatalog()->getSources().at(nodeId);
            nodeJsonValue = GeoNodeUtils::generateJson(source);
        }

        successMessageImpl(message, nodeJsonValue);
    } else {
        NES_ERROR("LocationController: Unable to find a " << path << " with id: " << nodeId);
        json::value errorResponse{};
        errorResponse["detail"] = json::value::string("Unable to find a " + path + " with id: " + nodeId);
        badRequestImpl(message, errorResponse);
    }
}

void LocationController::handleAddNode(web::http::http_request message, const std::string& path) {
    NES_DEBUG("LocationController: Adding new geo node");

    json::value req;

    message.extract_string(true).then([ message, &req](utility::string_t body) {
        std::string userRequest(body.begin(), body.end());
        NES_DEBUG("LocationController: userRequest: " + userRequest);
        req = json::value::parse(userRequest);
    })
    .wait();

    std::string nodeId = req.at("nodeId").as_string();

    if(path == "sink") {
        NES_DEBUG("LocationController: adding geo sink ...");
        double movingRange = req.at("movingRange").as_double();
        this->locationService->addSink(nodeId, movingRange);
        successMessageImpl(message, "Geo sink added!");
    }

    if(path == "source") {
        NES_DEBUG("LocationController: adding geo source ...");
        this->locationService->addSource(nodeId);
        successMessageImpl(message, "Geo source added!");
    }
}

void LocationController::handleUpdateLocation(web::http::http_request message) {
    NES_DEBUG("LocationController: Update location");
    message.extract_string(true).then([this, message](utility::string_t body) {
        std::string userRequest(body.begin(), body.end());
        NES_DEBUG("LocationController: userRequest: " + userRequest);

        json::value req = json::value::parse(userRequest);
        std::string nodeId = req.at("nodeId").as_string();

        double latitude = req.at("latitude").as_double();
        double longitude = req.at("longitude").as_double();

        this->locationService->updateNodeLocation(nodeId, std::make_shared<GeoPoint>(latitude, longitude));
        successMessageImpl(message, "Node location updated!");
    })
    .wait();
}

}// namespace NES
