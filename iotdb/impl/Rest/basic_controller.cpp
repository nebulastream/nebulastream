#include "Rest/basic_controller.hpp"
#include "Rest/network_utils.hpp"

namespace iotdb {
    BasicController::BasicController() {

    }

    BasicController::~BasicController() {

    }
    void BasicController::setEndpoint(const std::string & value) {
        std::cout << "Defining endpoint using " +  value << std::endl;
        uri endpointURI(value);
        uri_builder endpointBuilder;

        endpointBuilder.set_scheme(endpointURI.scheme());
        endpointBuilder.set_host(endpointURI.host());

        endpointBuilder.set_port(endpointURI.port());
        endpointBuilder.set_path(endpointURI.path());

        _listener = http_listener(endpointBuilder.to_uri());
    }

    std::string BasicController::endpoint() const {
        return _listener.uri().to_string();
    }

    pplx::task<void> BasicController::accept() {
        initRestOpHandlers();
        return _listener.open();
    }

    pplx::task<void> BasicController::shutdown() {
        return _listener.close();
    }

    std::vector<utility::string_t> BasicController::requestPath(const http_request & message) {
        auto relativePath = uri::decode(message.relative_uri().path());
        return uri::split_path(relativePath);        
    }
}