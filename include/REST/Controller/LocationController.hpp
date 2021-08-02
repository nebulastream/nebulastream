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

#ifndef NES_LOCATIONCONTROLLER_HPP
#define NES_LOCATIONCONTROLLER_HPP

#pragma once

#include <REST/Controller/BaseController.hpp>
#include <memory>
#include <cpprest/http_msg.h>

namespace NES {

class LocationService;
using LocationServicePtr = std::shared_ptr<LocationService>;

class LocationController : public BaseController {

  private:
    LocationServicePtr locationService;
    void handleGetLocations(web::http::http_request message);
    void handleAddNode(web::http::http_request message);
    void handleUpdateLocation(web::http::http_request message);

  public:
    explicit LocationController(LocationServicePtr  locationService);

    /**
     * Handling the Get requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handleGet(const std::vector<utility::string_t>& paths, const web::http::http_request& message);


    /**
     * Handling the Post requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

};

using LocationControllerPtr = std::shared_ptr<LocationController>;

}



#endif//NES_LOCATIONCONTROLLER_HPP
