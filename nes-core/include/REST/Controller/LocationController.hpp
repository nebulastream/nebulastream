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

#ifndef NES_INCLUDE_REST_CONTROLLER_LOCATIONCONTROLLER_HPP
#define NES_INCLUDE_REST_CONTROLLER_LOCATIONCONTROLLER_HPP
#include <REST/Controller/BaseController.hpp>
#include <memory>
#include <optional>

namespace NES {

namespace Spatial::Index::Experimental {
class LocationService;
using LocationServicePtr = std::shared_ptr<LocationService>;
}// namespace Spatial::Index::Experimental

class LocationController : public BaseController {
  public:
    LocationController(NES::Spatial::Index::Experimental::LocationServicePtr locationService);

    /**
     * Handling the Get requests for the locations
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handleGet(const std::vector<utility::string_t>& path, web::http::http_request& message) override;

  private:
    /**
     * Extracts the node id from the http requests parameters. if the parameter is not found or can not be converted to uint64_t,
     * the appropriate error response will be send and the return value will be nullopt_t
     * @param parameters: a map containing the string parameters
     * @param httpRequest:
     * @return : an optional containing the id or a nullopt_t if an error occurred.
     */
    static std::optional<uint64_t> getNodeIdFromURIParameter(std::map<utility::string_t, utility::string_t> parameters,
                                                             const web::http::http_request& httpRequest);

    NES::Spatial::Index::Experimental::LocationServicePtr locationService;
};
}// namespace NES
#endif//NES_INCLUDE_REST_CONTROLLER_LOCATIONCONTROLLER_HPP
