#pragma once

#include "BaseController.hpp"
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Actors/CoordinatorActor.hpp>
#include <Services/NESTopologyService.hpp>
#include <Services/QueryService.hpp>
#include <cpprest/http_msg.h>

namespace NES {

class QueryController : public BaseController {
  public:
    QueryController() {
        coordinatorServicePtr = CoordinatorService::getInstance();
        queryServicePtr = QueryService::getInstance();
        nesTopologyServicePtr = NESTopologyService::getInstance();
    }

    ~QueryController() {}

    void setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle);
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    QueryServicePtr queryServicePtr;
    NESTopologyServicePtr nesTopologyServicePtr;
    CoordinatorServicePtr coordinatorServicePtr;
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle;
};

}// namespace NES
