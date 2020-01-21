#pragma once

#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include "Actors/CoordinatorActor.hpp"
#include "Actors/Configurations/CoordinatorActorConfig.hpp"
#include <string>
#include <cpprest/http_listener.h>
#include <cpprest/details/http_server.h>
#include <pplx/pplxtasks.h>
#include "REST/controller.hpp"
#include "QueryController.hpp"
#include "StreamCatalogController.hpp"

using namespace web;
using namespace http;
using namespace http::experimental::listener;

namespace NES {
class BaseRestController : public Controller {
  protected:
    http_listener _listener; // main micro service network endpoint
    QueryController queryController;
    StreamCatalogController streamCatalogController;

  public:
    BaseRestController() {};
    ~BaseRestController() {};

    void handleGet(http_request message) override;
    void handlePut(http_request message) override;
    void handlePost(http_request message) override;
    void handlePatch(http_request message) override;
    void handleDelete(http_request message) override;
    void handleHead(http_request message) override;
    void handleOptions(http_request message) override;
    void handleTrace(http_request message) override;
    void handleConnect(http_request message) override;
    void handleMerge(http_request message) override;
    void setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle);
    void initRestOpHandlers();
    void setEndpoint(const std::string &value);
    std::string endpoint() const;
    pplx::task<void> accept();
    pplx::task<void> shutdown();
    json::value responseNotImpl(const http::method& method, utility::string_t path ="NES");
    std::vector<utility::string_t> splitPath(utility::string_t path);
    utility::string_t getPath(http_request &message);
};
}