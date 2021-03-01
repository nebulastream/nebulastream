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
//
// Created by balint on 27.02.21.
//

#ifndef NES_MAINTENANCECONTROLLER_HPP
#define NES_MAINTENANCECONTROLLER_HPP

#include "Services/MaintenanceService.hpp"
#include <REST/Controller/BaseController.hpp>
#include <cpprest/http_msg.h>

namespace NES{


class MaintenanceController: public BaseController{

  public:
    explicit MaintenanceController(MaintenanceServicePtr maintenanceService);

    /**
     * Handles Post request for marking nodes for maintenance
     * @param paths : the url of the rest request
     * @param message : the user message
     */
    void handlePost(std::vector<utility::string_t> paths, web::http::http_request message);

  private:
    MaintenanceServicePtr maintenanceService;


};
typedef std::shared_ptr<MaintenanceController> MaintenanceControllerPtr;
}//namespace NES
#endif//NES_MAINTENANCECONTROLLER_HPP
