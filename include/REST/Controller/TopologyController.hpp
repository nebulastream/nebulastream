#pragma once

#include <REST/Controller/BaseController.hpp>
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

class TopologyController : public BaseController {
  public:
    explicit TopologyController(TopologyPtr topology);

    ~TopologyController() = default;
    /**
     * Handling the Get requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handleGet(std::vector<utility::string_t> path, web::http::http_request message);

  private:
    TopologyPtr topology;
};

typedef std::shared_ptr<TopologyController> TopologyControllerPtr;
}// namespace NES
