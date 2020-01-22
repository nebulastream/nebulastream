
#ifndef NES_INCLUDE_REST_CONTROLLER_STREAMCATALOGCONTROLLER_HPP_
#define NES_INCLUDE_REST_CONTROLLER_STREAMCATALOGCONTROLLER_HPP_

#include <Services/StreamCatalogService.hpp>
#include <cpprest/details/basic_types.h>
#include <cpprest/http_msg.h>
#include <cpprest/json.h>
#include "BaseController.hpp"

namespace NES {

class StreamCatalogController : public BaseController {

  public:
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);
    void handleDelete(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    StreamCatalogService streamCatalogService;

};
}

#endif //NES_INCLUDE_REST_CONTROLLER_STREAMCATALOGCONTROLLER_HPP_
