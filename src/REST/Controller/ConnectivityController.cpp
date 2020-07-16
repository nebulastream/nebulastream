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
