#include <REST/Controller/MonitoringController.hpp>

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Services/MonitoringService.hpp>
#include <Util/Logger.hpp>

#include <Util/UtilityFunctions.hpp>
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
            auto metricsJson = monitoringService->requestMonitoringDataForAllNodes(nullptr);
            successMessageImpl(message, metricsJson);
            return;
        } else if (path.size() == 3) {
            auto strNodeId = path[2];
            NES_DEBUG("MonitoringController: GET metrics for " + strNodeId);
            if (!strNodeId.empty() && std::all_of(strNodeId.begin(), strNodeId.end(), ::isdigit)) {
                //if the arg is a valid number and node id
                uint64_t nodeId = std::stoi(path[2]);
                try {
                    //TODO: Add support of monitoring plan in the future
                    auto metricJson = monitoringService->requestMonitoringData(nodeId, nullptr);
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

}// namespace NES