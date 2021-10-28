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

#ifndef NES_INCLUDE_REST_CONTROLLER_MONITORING_CONTROLLER_HPP_
#define NES_INCLUDE_REST_CONTROLLER_MONITORING_CONTROLLER_HPP_

#include <REST/Controller/BaseController.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <memory>

namespace NES {

class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

class MonitoringController : public BaseController {

  public:
    explicit MonitoringController(MonitoringServicePtr mService, Runtime::BufferManagerPtr bufferManager);

    /**
     * Handling the Get requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handleGet(std::vector<utility::string_t> path, const web::http::http_request& message);

    /**
     * Handling the Post requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    MonitoringServicePtr monitoringService;
    Runtime::BufferManagerPtr bufferManager;
};

using MonitoringControllerPtr = std::shared_ptr<MonitoringController>;

}// namespace NES

#endif// NES_INCLUDE_REST_CONTROLLER_MONITORING_CONTROLLER_HPP_
