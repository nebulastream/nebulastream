#pragma once

#include "BaseController.hpp"
#include <Services/QueryService.hpp>
#include <cpprest/http_msg.h>

namespace NES {
class NesCoordinator;
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;
class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;
class TopologyManager;
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;
class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class QueryController : public BaseController {
  public:
    QueryController(NesCoordinatorPtr coordinator,
                    QueryCatalogPtr queryCatalog,
                    TopologyManagerPtr topologyManager,
                    StreamCatalogPtr streamCatalog) : coordinator(coordinator),
                                                      queryCatalog(queryCatalog),
                                                      topologyManager(topologyManager),
                                                      streamCatalog(streamCatalog) {
        queryServicePtr = std::make_shared<QueryService>(streamCatalog);
    }

    ~QueryController() {}

    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    TopologyManagerPtr topologyManager;
    QueryCatalogPtr queryCatalog;
    QueryServicePtr queryServicePtr;
    NesCoordinatorPtr coordinator;
    StreamCatalogPtr streamCatalog;
};

typedef std::shared_ptr<QueryController> QueryControllerPtr;

}// namespace NES
