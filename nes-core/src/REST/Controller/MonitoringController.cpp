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

#include <REST/Controller/MonitoringController.hpp>
#include <REST/Controller/BaseController.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/MonitoringService.hpp>
#include <Util/Logger/Logger.hpp>

#include <cpprest/http_client.h>
#include <string>
#include <vector>

namespace NES {

MonitoringController::MonitoringController(MonitoringServicePtr mService, Runtime::BufferManagerPtr bufferManager)
    : monitoringService(mService), bufferManager(bufferManager) {
    NES_DEBUG("MonitoringController: Initializing");
}

void MonitoringController::handleGet(const std::vector<utility::string_t>& path, web::http::http_request& message) {
    NES_INFO("MonitoringController: Processing GET request");
    if (path[1] == "start") {
        NES_DEBUG("MonitoringController: GET start monitoring streams");
        auto metricsJsonLohmann = monitoringService->startMonitoringStreams();
        web::json::value metricsJson = web::json::value::parse(metricsJsonLohmann.dump());
        successMessageImpl(message, metricsJson);
        return;
    } else if (path[1] == "stop") {
        NES_DEBUG("MonitoringController: GET stop monitoring streams");
        auto metricsJsonLohmann = monitoringService->stopMonitoringStreams();
        web::json::value response = web::json::value::parse(metricsJsonLohmann.dump());
        successMessageImpl(message, response);
        return;
    } else if (path[1] == "streams") {
        NES_DEBUG("MonitoringController: GET monitoring streams");
        auto metricsJsonLohmann = monitoringService->getMonitoringStreams();
        web::json::value metricsJson = web::json::value::parse(metricsJsonLohmann.dump());
        successMessageImpl(message, metricsJson);
        return;
    } else if (path[1] == "storage") {
        NES_DEBUG("MonitoringController: GET content of metric store");
        auto metricsJsonLohmann = monitoringService->requestNewestMonitoringDataFromMetricStoreAsJson();
        web::json::value metricsJson = web::json::value::parse(metricsJsonLohmann.dump());
        successMessageImpl(message, metricsJson);
        return;
    } else if (path.size() > 1 && path.size() < 4 && path[1] == "metrics") {
        NES_DEBUG("MonitoringController: GET metrics with path size " + std::to_string(path.size()));
        if (path.size() == 2) {
            NES_DEBUG("MonitoringController: GET metrics for all nodes");
            auto metricsJsonLohmann = monitoringService->requestMonitoringDataFromAllNodesAsJson();
            web::json::value metricsJson = web::json::value::parse(metricsJsonLohmann.dump());
            successMessageImpl(message, metricsJson);
            return;
        }
        if (path.size() == 3) {
            auto strNodeId = path[2];

            if (!strNodeId.empty() && std::all_of(strNodeId.begin(), strNodeId.end(), ::isdigit)) {
                NES_DEBUG("MonitoringController: GET metrics for node " + strNodeId);
                //if the arg is a valid number and node id
                uint64_t nodeId = std::stoi(path[2]);
                try {
                    auto metricsJsonLohmann = monitoringService->requestMonitoringDataAsJson(nodeId);
                    web::json::value metricsJson = web::json::value::parse(metricsJsonLohmann.dump());
                    successMessageImpl(message, metricsJson);
                } catch (std::runtime_error& ex) {
                    std::string errorMsg = ex.what();
                    NES_ERROR("MonitoringController: GET metrics error: " + errorMsg);
                    message.reply(web::http::status_codes::BadRequest, ex.what());
                }
            } else {
                NES_DEBUG("MonitoringController: GET metrics for " + strNodeId + " invalid");
                message.reply(web::http::status_codes::BadRequest, "The provided node ID " + path[2] + " is not valid.");
            }
            return;
        }
    }
    resourceNotFoundImpl(message);
}

void MonitoringController::handlePost(const std::vector<utility::string_t>&, web::http::http_request& message) {
    NES_DEBUG("MonitoringController: Processing POST request");
    resourceNotFoundImpl(message);
}

}// namespace NES