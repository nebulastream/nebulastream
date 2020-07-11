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

    explicit QueryController(QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager,
                             StreamCatalogPtr streamCatalog, GlobalExecutionPlanPtr globalExecutionPlan);

    ~QueryController() = default;

    /**
     * Handling the Get requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);

    /**
     * Handling the POST requests for the query
     * @param path: the url of the rest request
     * @param message: the user message
     */
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

    /**
     * @brief Handle the stop request for the query
     * @param path : the resource path the user wanted to get
     * @param message : the user message
     */
    void handleDelete(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    QueryCatalogPtr queryCatalog;
    TopologyManagerPtr topologyManager;
    QueryServicePtr queryService;
    StreamCatalogPtr streamCatalog;
    GlobalExecutionPlanPtr globalExecutionPlan;
};

typedef std::shared_ptr<QueryController> QueryControllerPtr;

}// namespace NES
