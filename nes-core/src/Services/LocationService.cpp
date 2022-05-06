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

#include <Services/LocationService.hpp>
#include <cpprest/json.h>
#include <Spatial/LocationIndex.hpp>
#include <Common/Location.hpp>

namespace NES::Spatial::Index::Experimental {
LocationService::LocationService(LocationIndexPtr locationIndex) : locationIndex(locationIndex) {};

web::json::value LocationService::requestLocationDataFromAllMobileNodesAsJson() {
    auto nodeVector = locationIndex->getMobileNodeLocations();
    web::json::value locMapJson;
    size_t count = 0;
    for (const auto& elem : nodeVector) {
        web::json::value nodeInfo;
        nodeInfo["id"] = web::json::value(elem.first);
        web::json::value locJson;
        locJson[0] = elem.second.getLatitude();
        locJson[1] = elem.second.getLongitude();
        nodeInfo["location"] = web::json::value(locJson);
        locMapJson[count] = web::json::value(nodeInfo);
        ++count;
    }
    return locMapJson;
}
}
