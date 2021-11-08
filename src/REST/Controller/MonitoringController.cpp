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

#include <REST/Controller/MonitoringController.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/MonitoringService.hpp>
#include <Util/Logger.hpp>

#include <REST/runtime_utils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/http_client.h>
#include <string>
#include <utility>
#include <vector>

namespace NES {

MonitoringController::MonitoringController(MonitoringServicePtr mService, Runtime::BufferManagerPtr bufferManager)
    : monitoringService(mService), bufferManager(bufferManager) {
    NES_DEBUG("MonitoringController: Initializing");
}

void MonitoringController::handleGet(const std::vector<utility::string_t>& path, web::http::http_request& message) {
    NES_DEBUG("MonitoringController: Processing GET request");
    if (path.size() > 1 && path.size() < 4 && path[1] == "metrics") {
        NES_DEBUG("MonitoringController: GET metrics with path size " + std::to_string(path.size()));
        if (path.size() == 2) {
            NES_DEBUG("MonitoringController: GET metrics for all nodes");
            auto metricsJson = monitoringService->requestMonitoringDataFromAllNodesAsJson(bufferManager);
            successMessageImpl(message, metricsJson);
            return;
        }
        if (path.size() == 3) {
            auto strNodeId = path[2];

            if (strNodeId == "prometheus") {
                NES_DEBUG("MonitoringController: GET metrics for all nodes via prometheus");
                NES_DEBUG("MonitoringController: handlePost -metrics: Querying all nodes via prometheus node exporter");
                auto metricsJson = monitoringService->requestMonitoringDataFromAllNodesViaPrometheusAsJson();
                successMessageImpl(message, metricsJson);
            } else if (!strNodeId.empty() && std::all_of(strNodeId.begin(), strNodeId.end(), ::isdigit)) {
                NES_DEBUG("MonitoringController: GET metrics for node " + strNodeId);
                //if the arg is a valid number and node id
                uint64_t nodeId = std::stoi(path[2]);
                try {
                    auto metricJson = monitoringService->requestMonitoringDataAsJson(nodeId, bufferManager);
                    successMessageImpl(message, metricJson);
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

void MonitoringController::handlePost(const std::vector<utility::string_t>& path, web::http::http_request& message) {
    NES_DEBUG("MonitoringController: Processing POST request");
    if (path.size() == 2 && path[1] == "metrics") {
        NES_DEBUG("MonitoringController: Processing POST metrics with path size " + std::to_string(path.size()));

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    std::string userRequest(body.begin(), body.end());
                    NES_DEBUG("MonitoringController: handlePost -metrics: userRequest: " + userRequest);
                    web::json::value req = web::json::value::parse(userRequest);

                    std::string endpoint = req.at("endpoint").as_string();

                    if (endpoint == "prometheus") {
                        if (req.has_string_field("node-id")) {
                            //if node-id specified, request metrics from specified node
                            auto strNodeId = req.at("node-id").as_string();
                            if (!strNodeId.empty() && std::all_of(strNodeId.begin(), strNodeId.end(), ::isdigit)) {
                                //if the arg is a valid number and node id
                                uint64_t nodeId = std::stoi(strNodeId);
                                uint16_t promPort = 0;

                                if (req.has_field("prom-port")) {
                                    promPort = req.at("prom-port").as_integer();
                                } else {
                                    promPort = 9100;
                                }

                                try {
                                    auto metricPlain =
                                        monitoringService->requestMonitoringDataViaPrometheusAsString(nodeId, promPort);
                                    successMessageImpl(message, metricPlain);
                                } catch (std::runtime_error& ex) {
                                    NES_ERROR("MonitoringController: POST metrics error: " << ex.what());
                                    message.reply(web::http::status_codes::BadRequest, ex.what());
                                }
                            } else {
                                NES_DEBUG("MonitoringController: POST metrics for " + strNodeId + " invalid");
                                message.reply(web::http::status_codes::BadRequest, "The provided node ID " + strNodeId + " is not valid.");
                            }
                            return;
                        }//otherwise get metrics from all nodes via prometheus
                        NES_DEBUG("MonitoringController: handlePost -metrics: Querying all nodes via prometheus node exporter");
                        auto metricsJson = monitoringService->requestMonitoringDataFromAllNodesViaPrometheusAsJson();
                        successMessageImpl(message, metricsJson);
                        return;

                    } else {
                        NES_DEBUG("MonitoringController: handlePost -metrics: unable to determine endpoint " + endpoint);
                        throw std::invalid_argument("Unable to determine endpoint " + endpoint);
                    }
                } catch (const std::exception& exc) {
                    NES_ERROR("MonitoringController: Exception occurred during handlePost -metrics: " << exc.what());
                    message.reply(web::http::status_codes::BadRequest, exc.what());
                    return;
                } catch (...) {
                    RuntimeUtils::printStackTrace();
                    internalServerErrorImpl(message);
                    return;
                }
            })
            .wait();
    }
    resourceNotFoundImpl(message);
}

}// namespace NES