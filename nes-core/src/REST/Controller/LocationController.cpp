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

namespace NES::Spatial::Index::Experimental {

LocationController::LocationController(LocationServicePtr locationService) : locationService(std::move(locationService)) {
    NES_DEBUG("LocationController: Initializing");
}

void LocationController::handleGet(const std::vector<utility::string_t>& path, web::http::http_request& message) {
    NES_DEBUG("LocationController: Processing GET request")
    //todo: check max length of path (3?)
    if (path.size() > 1 && path.size() < 4) {
        if (path[1] == "allMobile") {
            NES_DEBUG("LocationController: GET location of all mobile nodes")

        }
        //todo: implement getter for individual nodes
        resourceNotFoundImpl(message);
    }
    resourceNotFoundImpl(message);
}
}