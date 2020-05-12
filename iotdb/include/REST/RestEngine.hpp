#pragma once

#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include "Actors/Configurations/CoordinatorActorConfig.hpp"
#include "Actors/CoordinatorActor.hpp"
#include "REST/Controller/BaseController.hpp"
#include "REST/Controller/QueryController.hpp"
#include "REST/Controller/StreamCatalogController.hpp"
#include <REST/Controller/QueryCatalogController.hpp>
#include <cpprest/details/http_server.h>
#include <cpprest/http_listener.h>
#include <pplx/pplxtasks.h>
#include <string>


using namespace web;
using namespace http;
using namespace http::experimental::listener;

namespace NES {
class NesCoordinator;
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

class RestEngine : public BaseController {
  protected:
    http_listener _listener;// main micro service network endpoint

  private:
    QueryController queryController;
    StreamCatalogController streamCatalogController;
    QueryCatalogController queryCatalogController;

  public:
    RestEngine(){
        NES_DEBUG("RestEngine");
    };
    ~RestEngine(){
        NES_DEBUG("~RestEngine");
    };

    void handleGet(http_request message);
    void handlePost(http_request message);
    void handleDelete(http_request message);
    void handlePut(http_request message);
    void handlePatch(http_request message);
    void handleHead(http_request message);
    void handleTrace(http_request message);
    void handleMerge(http_request message);

    void setCoordinator(NesCoordinatorPtr coordinator);
    void initRestOpHandlers();
    void setEndpoint(const std::string& value);
    std::string endpoint() const;
    pplx::task<void> accept();
    pplx::task<void> shutdown();
    std::vector<utility::string_t> splitPath(utility::string_t path);
};
}// namespace NES