#pragma once

#include <Actors/CoordinatorActor.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Services/QueryService.hpp>
#include <Services/NESTopologyService.hpp>
#include <cpprest/http_msg.h>

namespace NES {

class QueryController {
  public:
    QueryController() {
        coordinatorServicePtr = CoordinatorService::getInstance();
    }
    ~QueryController() {}

    void setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle);
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);
  private:

    QueryService queryService;
    NESTopologyService nesTopologyService;
    CoordinatorServicePtr coordinatorServicePtr;
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle;

    void internalServerErrorImpl(const web::http::http_request& message) const;
    void successMessageImpl(const http::http_request& message, const json::value& result) const;
    void resourceNotFoundImpl(const http::http_request& message) const;
};

}

