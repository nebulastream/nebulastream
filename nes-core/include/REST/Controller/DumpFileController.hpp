//
// Created by nes on 1/24/22.
//

#ifndef NES_NES_CORE_SRC_REST_CONTROLLER_DUMPFILECONTROLLER_HPP_
#define NES_NES_CORE_SRC_REST_CONTROLLER_DUMPFILECONTROLLER_HPP_

#include "BaseController.hpp"
#include "REST/CpprestForwardedRefs.hpp"
#include "REST/RestEngine.hpp"

namespace NES {

class DumpFileController : public BaseController {
  public:
    explicit DumpFileController(TopologyPtr topology, WorkerRPCClientPtr workerClient);

    ~DumpFileController() = default;
    /**
     * Handling the Get requests for the query
     * @param path : the url of the rest request
     * @param message : the user message
     */
    void handleGet(const std::vector<utility::string_t>& path, web::http::http_request& message) override;

  private:
    TopologyPtr topology;
    WorkerRPCClientPtr workerClient;
};

using DumpFileControllerPtr = std::shared_ptr<DumpFileController>;
}// namespace NES
#endif//NES_NES_CORE_SRC_REST_CONTROLLER_DUMPFILECONTROLLER_HPP_