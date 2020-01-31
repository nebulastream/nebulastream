#ifndef NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_

#include <Actors/CoordinatorActor.hpp>
#include <Services/QueryCatalogService.hpp>
#include "BaseController.hpp"

namespace NES {

class QueryCatalogController: public BaseController {

  public:
    QueryCatalogController(){
        queryCatalogServicePtr = QueryCatalogService::getInstance();
    }

    void setCoordinatorActorHandle(infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle);
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handleDelete(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle;
    QueryCatalogServicePtr queryCatalogServicePtr;
};

}
#endif //NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
