#ifndef NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_

#include "BaseController.hpp"
#include <Actors/CoordinatorActor.hpp>
#include <Services/QueryCatalogService.hpp>
namespace NES {
class NesCoordinator;
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

class QueryCatalogController : public BaseController {

  public:
    QueryCatalogController() {
        queryCatalogServicePtr = QueryCatalogService::getInstance();
    }

    void setCoordinator(NesCoordinatorPtr coordinator);
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handleDelete(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    NesCoordinatorPtr coordinator;
    QueryCatalogServicePtr queryCatalogServicePtr;
};

}// namespace NES
#endif//NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
