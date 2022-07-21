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

#ifndef NES_MAINTENANCECONTROLLER_HPP
#define NES_MAINTENANCECONTROLLER_HPP

#include <REST/Controller/BaseController.hpp>
#include <Services/MaintenanceService.hpp>
#include <cpprest/http_msg.h>

namespace NES::Experimental {

/**
 * @brief this class represents the web service handler for maintenance requests
 */
class MaintenanceController : public BaseController {

  public:
    explicit MaintenanceController(MaintenanceServicePtr maintenanceService);

    /**
     * Handles Post request for marking nodes for maintenance
     * @param paths : the url of the rest request
     * @param message : the user message
     */
    void handlePost(const std::vector<utility::string_t>& path, web::http::http_request& request) override;

  private:
    MaintenanceServicePtr maintenanceService;
};

}//namespace NES::Experimental
#endif//NES_MAINTENANCECONTROLLER_HPP
