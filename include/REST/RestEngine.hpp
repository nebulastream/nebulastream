#pragma once

#if defined(__APPLE__) || defined(__MACH__)
#include <xlocale.h>
#endif
#include "REST/Controller/BaseController.hpp"
#include "REST/Controller/QueryController.hpp"
#include "REST/Controller/StreamCatalogController.hpp"
#include <REST/Controller/ConnectivityController.hpp>
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

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;

class RestEngine : public BaseController {
  protected:
    http_listener _listener;// main micro service network endpoint

  private:
    QueryControllerPtr queryController;
    QueryCatalogControllerPtr queryCatalogController;
    StreamCatalogPtr streamCatalog;
    StreamCatalogControllerPtr streamCatalogController;
    ConnectivityControllerPtr connectivityController;

  public:
    RestEngine(StreamCatalogPtr streamCatalog, NesCoordinatorPtr coordinator, QueryCatalogPtr queryCatalog,
               TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, QueryServicePtr queryService);

    ~RestEngine();

    void handleGet(http_request message);
    void handlePost(http_request message);
    void handleDelete(http_request message);
    void handlePut(http_request message);
    void handlePatch(http_request message);
    void handleHead(http_request message);
    void handleTrace(http_request message);
    void handleMerge(http_request message);
    void initRestOpHandlers();
    void setEndpoint(const std::string& value);
    std::string endpoint() const;
    pplx::task<void> accept();
    pplx::task<void> shutdown();
    std::vector<utility::string_t> splitPath(utility::string_t path);
};

typedef std::shared_ptr<RestEngine> RestEnginePtr;
}// namespace NES