#ifndef NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_

#include "BaseController.hpp"
namespace NES {
class NesCoordinator;
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class QueryCatalogController : public BaseController {

  public:
    QueryCatalogController(QueryCatalogPtr queryCatalog,
                           NesCoordinatorPtr coordinator);

    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handleDelete(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    QueryCatalogPtr queryCatalog;
    NesCoordinatorPtr coordinator;
};

typedef std::shared_ptr<QueryCatalogController> QueryCatalogControllerPtr;

}// namespace NES
#endif//NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
