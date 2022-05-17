/*
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

#include "Util/Logger/Logger.hpp"
#include <REST/Controller/LocationController.hpp>
#include <utility>
#include <Services/LocationService.hpp>

namespace NES::Spatial::Index::Experimental {

const std::string kAllMobileLocationsRequestString = "allMobile";
const std::string kNodeIdParamString = "nodeId";

LocationController::LocationController(LocationServicePtr locationService) : locationService(std::move(locationService)) {
    NES_DEBUG("LocationController: Initializing");
}

void LocationController::handleGet(const std::vector<utility::string_t>& path, web::http::http_request& message) {
    NES_DEBUG("LocationController: Processing GET request")
    auto parameters = getParameters(message);
    //get the location of a node with a specific id
    if (path.size() == 1) {
        auto nodeIdOpt = getNodeIdFromURIParameter(parameters, message);
        if (!nodeIdOpt.has_value()) {
            return;
        }
        auto nodeLocationJson = locationService->requestNodeLocationDataAsJson(nodeIdOpt.value());
        if (nodeLocationJson == web::json::value::null()) {
            NES_ERROR("node with id " << std::to_string(nodeIdOpt.value()) << " does not exist");
            web::json::value errorResponse{};
            auto statusCode = web::http::status_codes::NotFound;
            errorResponse["code"] = web::json::value(statusCode);
            errorResponse["message"] = web::json::value::string("No node with this Id");
            errorMessageImpl(message, errorResponse, statusCode);
            return;
        }
        successMessageImpl(message, nodeLocationJson);
        return;
    }

    //get the locations of all mobile nodes
    if (path.size() > 1 && path.size() < 3) {
        if (path[1] == kAllMobileLocationsRequestString) {
            NES_DEBUG("LocationController: GET location of all mobile nodes")
            auto locationsJson = locationService->requestLocationDataFromAllMobileNodesAsJson();
            successMessageImpl(message, locationsJson);
            return;
        }
    }
    resourceNotFoundImpl(message);
}

std::optional<uint64_t> LocationController::getNodeIdFromURIParameter(std::map<utility::string_t, utility::string_t> parameters,
                                                                          const web::http::http_request& httpRequest) {
   auto const idParameter = parameters.find(kNodeIdParamString);
   if (idParameter == parameters.end()) {
       NES_ERROR("LocationController: Unable to find nodeId parameter in the GET request");
       web::json::value errorResponse{};
       auto statusCode = web::http::status_codes::BadRequest;
       errorResponse["code"] = web::json::value(statusCode);
       errorResponse["message"] = web::json::value::string("Parameter nodeId must be provided");
       errorMessageImpl(httpRequest, errorResponse, statusCode);
       return {};
   }
   uint64_t nodeId;
   try {
       nodeId = std::stoi(idParameter->second);
   } catch (const std::invalid_argument& invalidArgument) {
       NES_ERROR("LocationController: Unable to convert nodeId parameter to integer");
       web::json::value errorResponse{};
       auto statusCode = web::http::status_codes::BadRequest;
       errorResponse["code"] = web::json::value(statusCode);
       errorResponse["message"] = web::json::value::string("Parameter nodeId must be an unsigned integer");
       errorMessageImpl(httpRequest, errorResponse, statusCode);
       return {};
   } catch (const std::out_of_range& outOfRange) {
       NES_ERROR("LocationController: nodeId is out of value range of the 64bit integer format");
       web::json::value errorResponse{};
       auto statusCode = web::http::status_codes::BadRequest;
       errorResponse["code"] = web::json::value(statusCode);
       errorResponse["message"] = web::json::value::string("Parameter nodeId must be in 64bit unsigned int value range");
       errorMessageImpl(httpRequest, errorResponse, statusCode);
       return {};
   }
   return nodeId;
}
}