#pragma once

#include <REST/Controller/BaseController.hpp>
#include <Services/QueryService.hpp>
#include <Services/FogTopologyService.hpp>
#include <REST/controller.hpp>

using namespace iotdb;

class RestController : public BaseController, Controller {
public:
    RestController() : BaseController() {}
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
    static json::value responseNotImpl(const http::method & method);
    QueryService queryService;
    FogTopologyService fogTopologyService;
};