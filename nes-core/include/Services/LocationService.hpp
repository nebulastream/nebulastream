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
#ifndef NES_INCLUDE_SERVICES_LOCATIONSERVICE_HPP
#define NES_INCLUDE_SERVICES_LOCATIONSERVICE_HPP

#include <memory>
#include <cpprest/json.h>
namespace web::json {
class value;
}// namespace web::json

namespace NES::Spatial::Index::Experimental {
class LocationIndex;
using LocationIndexPtr = std::shared_ptr<LocationIndex>;

class LocationService {
  public:
    explicit LocationService(LocationIndexPtr locationIndex);

    /**
     * @brief get a list of all mobile nodes in the system and their current positions
     * @return a json list in the format:
     * [
            {
                "id": <node id>,
                "location": [
                    <latitude>,
                    <longitude>
                ]
            }
        ]
     */
    web::json::value requestLocationDataFromAllMobileNodesAsJson();

  private:
    LocationIndexPtr locationIndex;
};
}

#endif//NES_INCLUDE_SERVICES_LOCATIONSERVICE_HPP
