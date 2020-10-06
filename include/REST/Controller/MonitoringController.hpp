#ifndef NES_INCLUDE_REST_CONTROLLER_MONITORINGCONTROLLER_HPP_
#define NES_INCLUDE_REST_CONTROLLER_MONITORINGCONTROLLER_HPP_

#include <REST/Controller/BaseController.hpp>
#include <memory>

namespace NES {

class MonitoringService;
typedef std::shared_ptr<MonitoringService> MonitoringServicePtr;

class MonitoringController : public BaseController {

  public:
    MonitoringController(MonitoringServicePtr mService);

    /**
     * Handling the Get requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);

    /**
     * Handling the Post requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    MonitoringServicePtr monitoringService;
};

typedef std::shared_ptr<MonitoringController> MonitoringControllerPtr;

}// namespace NES

#endif//NES_INCLUDE_REST_CONTROLLER_MONITORINGCONTROLLER_HPP_
