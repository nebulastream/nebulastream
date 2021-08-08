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
        if (paths.size() == 1) {
            handleGetLocations(message);
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
        if (path.size() == 1) {
            handleAddNode(message);
            return;
        }
        if (path.size() == 2 && path[1] == "update") {
            handleUpdateLocation(message);
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
    std::vector<web::json::value> nodes = {};
    for (auto const& [nodeId, node] : locationService->getLocationCatalog()->getSources()) {
        web::json::value currentNodeJsonValue{};
        currentNodeJsonValue["id"] = web::json::value::string(node->getId());
        currentNodeJsonValue["latitude"] = web::json::value::number(node->getCurrentLocation()->getLatitude());
        currentNodeJsonValue["longitude"] = web::json::value::number(node->getCurrentLocation()->getLongitude());

        nodes.push_back(currentNodeJsonValue);
    }

    web::json::value responseJson = web::json::value::array(nodes);
    successMessageImpl(message, responseJson);
}

void LocationController::handleAddNode(web::http::http_request message) {
    NES_DEBUG("LocationController: Adding new resource");
    message.extract_string(true).then([this, message](utility::string_t body) {
        std::string userRequest(body.begin(), body.end());
        NES_DEBUG("LocationController: userRequest: " + userRequest);
        json::value req = json::value::parse(userRequest);
        std::string nodeId = req.at("nodeId").as_string();
        this->locationService->addSource(nodeId);
        successMessageImpl(message, "New node added!");
    })
    .wait();
}

void LocationController::handleUpdateLocation(web::http::http_request message) {
    NES_DEBUG("LocationController: Update resource");
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
