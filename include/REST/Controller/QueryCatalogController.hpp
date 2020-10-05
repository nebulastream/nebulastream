#ifndef NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_

#include <REST/Controller/BaseController.hpp>

namespace NES {
class NesCoordinator;
typedef std::weak_ptr<NesCoordinator> NesCoordinatorWeakPtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class QueryCatalogController : public BaseController {

  public:
    QueryCatalogController(QueryCatalogPtr queryCatalog, NesCoordinatorWeakPtr coordinator, GlobalQueryPlanPtr globalQueryPlan);

    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    QueryCatalogPtr queryCatalog;
    NesCoordinatorWeakPtr coordinator;
    GlobalQueryPlanPtr globalQueryPlan;
};

typedef std::shared_ptr<QueryCatalogController> QueryCatalogControllerPtr;
}// namespace NES
#endif//NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
