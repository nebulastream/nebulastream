
#ifndef NES_INCLUDE_REST_CONTROLLER_STREAMCATALOGCONTROLLER_HPP_
#define NES_INCLUDE_REST_CONTROLLER_STREAMCATALOGCONTROLLER_HPP_

#include "BaseController.hpp"
#include <Services/StreamCatalogService.hpp>
#include <cpprest/details/basic_types.h>
#include <cpprest/http_msg.h>
#include <cpprest/json.h>

namespace NES {

class StreamCatalogController : public BaseController {

  public:
    StreamCatalogController() {
        streamCatalogServicePtr = StreamCatalogService::getInstance();
    }

    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);
    void handlePost(std::vector<utility::string_t> path, web::http::http_request message);
    void handleDelete(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    StreamCatalogServicePtr streamCatalogServicePtr;
};
}// namespace NES

#endif//NES_INCLUDE_REST_CONTROLLER_STREAMCATALOGCONTROLLER_HPP_
