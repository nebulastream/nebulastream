#ifndef NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
#define NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_

#include "BaseController.hpp"
namespace NES {

class QueryCatalogController: public BaseController {

  public:
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handleDelete(std::vector<utility::string_t> path, web::http::http_request message);

  private:

};

}
#endif //NES_IMPL_REST_CONTROLLER_QUERYCATALOGCONTROLLER_HPP_
