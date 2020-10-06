#include <REST/Controller/MonitoringController.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Services/MonitoringService.hpp>
#include <Util/Logger.hpp>

#include <REST/runtime_utils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/http_client.h>
#include <string>
#include <vector>

namespace NES {

MonitoringController::MonitoringController(MonitoringServicePtr mService) : monitoringService(mService) {
    NES_DEBUG("MonitoringController: Initializing");
}

void MonitoringController::handleGet(std::vector<utility::string_t> path, http_request message) {
    NES_DEBUG("MonitoringController: Processing GET request");
    if (path.size() > 1 && path.size() < 4 && path[1] == "metrics") {
        NES_DEBUG("MonitoringController: GET metrics with path size " + std::to_string(path.size()));
        if (path.size() == 2) {
            NES_DEBUG("MonitoringController: GET metrics for all nodes");
            //TODO: Add support for monitoring plan in the future
            auto metricsJson = monitoringService->requestMonitoringDataForAllNodesAsJson(nullptr);
            successMessageImpl(message, metricsJson);
            return;
        } else if (path.size() == 3) {
            auto strNodeId = path[2];

            if (strNodeId == "prometheus") {
                NES_DEBUG("MonitoringController: GET metrics for all nodes via prometheus");
                //TODO: Add support of monitoring plan in the future
                NES_DEBUG("MonitoringController: handlePost -metrics: Querying all nodes via prometheus node exporter");
                auto metricsJson = monitoringService->requestMonitoringDataFromAllNodesViaPrometheusAsJson(nullptr);
                successMessageImpl(message, metricsJson);
            } else if (!strNodeId.empty() && std::all_of(strNodeId.begin(), strNodeId.end(), ::isdigit)) {
                NES_DEBUG("MonitoringController: GET metrics for node " + strNodeId);
                //if the arg is a valid number and node id
                uint64_t nodeId = std::stoi(path[2]);
                try {
                    //TODO: Add support of monitoring plan in the future
                    auto metricJson = monitoringService->requestMonitoringDataAsJson(nodeId, nullptr);
                    successMessageImpl(message, metricJson);
                } catch (std::runtime_error& ex) {
                    NES_ERROR("MonitoringController: GET metrics error: " << ex.what());
                    message.reply(status_codes::BadRequest, ex.what());
                }
            } else {
                NES_DEBUG("MonitoringController: GET metrics for " + strNodeId + " invalid");
                message.reply(status_codes::BadRequest, "The provided node ID " + path[2] + " is not valid.");
            }
            return;
        }
    }
    resourceNotFoundImpl(message);
    return;
}

void MonitoringController::handlePost(std::vector<utility::string_t> path, web::http::http_request message) {
    NES_DEBUG("MonitoringController: Processing POST request");
    if (path.size() == 2 && path[1] == "metrics") {
        NES_DEBUG("MonitoringController: Processing POST metrics with path size " + std::to_string(path.size()));

        message.extract_string(true)
            .then([this, message](utility::string_t body) {
                try {
                    std::string userRequest(body.begin(), body.end());
                    NES_DEBUG("MonitoringController: handlePost -metrics: userRequest: " << userRequest);
                    json::value req = json::value::parse(userRequest);

                    std::string endpoint = req.at("endpoint").as_string();

                    if (endpoint == "prometheus") {
                        if (req.has_string_field("node-id")) {
                            //if node-id specified, request metrics from specified node
                            auto strNodeId = req.at("node-id").as_string();
                            if (!strNodeId.empty() && std::all_of(strNodeId.begin(), strNodeId.end(), ::isdigit)) {
                                //if the arg is a valid number and node id
                                uint64_t nodeId = std::stoi(strNodeId);
                                uint16_t promPort;

                                if (req.has_field("prom-port")) {
                                    promPort = req.at("prom-port").as_integer();
                                } else {
                                    promPort = 9100;
                                }

                                try {
                                    //TODO: Add support of monitoring plan in the future
                                    auto metricPlain = monitoringService->requestMonitoringDataViaPrometheusAsString(nodeId, promPort, nullptr);
                                    successMessageImpl(message, metricPlain);
                                } catch (std::runtime_error& ex) {
                                    NES_ERROR("MonitoringController: POST metrics error: " << ex.what());
                                    message.reply(status_codes::BadRequest, ex.what());
                                }
                            } else {
                                NES_DEBUG("MonitoringController: POST metrics for " + strNodeId + " invalid");
                                message.reply(status_codes::BadRequest, "The provided node ID " + strNodeId + " is not valid.");
                            }
                            return;
                        } else {
                            //otherwise get metrics from all nodes via prometheus
                            //TODO: Add support of monitoring plan in the future
                            NES_DEBUG("MonitoringController: handlePost -metrics: Querying all nodes via prometheus node exporter");
                            auto metricsJson = monitoringService->requestMonitoringDataFromAllNodesViaPrometheusAsJson(nullptr);
                            successMessageImpl(message, metricsJson);
                            return;
                        }
                    } else {
                        NES_DEBUG("MonitoringController: handlePost -metrics: unable to determine endpoint " + endpoint);
                        throw std::invalid_argument("Unable to determine endpoint " + endpoint);
                    }
                } catch (const std::exception& exc) {
                    NES_ERROR("MonitoringController: Exception occurred during handlePost -metrics: " << exc.what());
                    message.reply(status_codes::BadRequest, exc.what());
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
    return;
}

}// namespace NES