#pragma once

#include <REST/Controller/BaseController.hpp>
#include <Services/QueryService.hpp>
#include <cpprest/http_msg.h>

/*
- * The above undef ensures that NES will compile.
- * There is a 3rd-party library that defines U as a macro for some internal stuff.
- * U is also a template argument of a template function in boost.
- * When the compiler sees them both, it goes crazy.
- * Do not remove the above undef.
- */
#undef U

namespace NES {

class NesCoordinator;
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class TopologyManager;
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryController : public BaseController {
  public:
    explicit QueryController(NesCoordinatorPtr coordinator, QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager,
                             StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr executionPlan);

    ~QueryController() = default;

    /**
     * Handling the Get requests for 
     * @param path
     * @param message
     */
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);

    /**
     *
     * @param path
     * @param message
     */
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    NesCoordinatorPtr coordinator;
    QueryCatalogPtr queryCatalog;
    TopologyManagerPtr topologyManager;
    QueryServicePtr queryServicePtr;
    StreamCatalogPtr streamCatalog;
    GlobalExecutionPlanPtr executionPlan;
};

typedef std::shared_ptr<QueryController> QueryControllerPtr;

}// namespace NES
