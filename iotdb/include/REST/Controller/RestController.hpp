#pragma once

#include <REST/Controller/BaseController.hpp>
#include <REST/controller.hpp>
#include <Services/QueryService.hpp>
#include <Services/FogTopologyService.hpp>
#include <Services/CoordinatorService.hpp>

namespace iotdb {

class RestController : public BaseController, Controller {
 public:
  RestController() : BaseController() {
    coordinatorService = CoordinatorService::getInstance();
  }
  ~RestController() {}
  void handleGet(http_request message) override;
  void handlePut(http_request message) override;
  void handlePost(http_request message) override;
  void handlePatch(http_request message) override;
  void handleDelete(http_request message) override;
  void handleHead(http_request message) override;
  void handleOptions(http_request message) override;
  void handleTrace(http_request message) override;
  void handleConnect(http_request message) override;
  void handleMerge(http_request message) override;
  void initRestOpHandlers() override;

 private:
  static json::value responseNotImpl(const http::method &method);
  QueryService queryService;
  FogTopologyService fogTopologyService;
  OptimizerService optimizerService;
  CoordinatorServicePtr coordinatorService;
};

}

