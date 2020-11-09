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

#include <REST/Controller/ConnectivityController.hpp>
#include <REST/runtime_utils.hpp>
#include <REST/std_service.hpp>
#include <Util/Logger.hpp>

using namespace web;
using namespace http;

namespace NES {

ConnectivityController::ConnectivityController() {
}

void ConnectivityController::handleGet(std::vector<utility::string_t> path, http_request message) {
    if (path[1] == "check") {
        json::value result{};
        result["success"] = json::value::boolean(true);
        successMessageImpl(message, result);
    } else {
        resourceNotFoundImpl(message);
    }
}

}// namespace NES
