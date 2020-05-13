#pragma once

#include "BaseController.hpp"
#include <Services/NESTopologyService.hpp>
#include <Services/QueryService.hpp>
#include <cpprest/http_msg.h>
#include <Services/CoordinatorService.hpp>

namespace NES {
class NesCoordinator;
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

class QueryController : public BaseController {
  public:
    QueryController() {
        coordinatorServicePtr = CoordinatorService::getInstance();
        queryServicePtr = QueryService::getInstance();
        nesTopologyServicePtr = NESTopologyService::getInstance();
    }

    ~QueryController() {}

    void setCoordinator(NesCoordinatorPtr coordinator);
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    QueryServicePtr queryServicePtr;
    NESTopologyServicePtr nesTopologyServicePtr;
    CoordinatorServicePtr coordinatorServicePtr;
    NesCoordinatorPtr coordinator;
};

}// namespace NES
