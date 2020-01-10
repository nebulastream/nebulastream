#include "REST/Controller/StreamCatalogController.hpp"

namespace iotdb {

void StreamCatalogController::initRestOpHandlers() {
    _listener.support(methods::GET, std::bind(&StreamCatalogController::handleGet, this, std::placeholders::_1));
    _listener.support(methods::PUT, std::bind(&StreamCatalogController::handlePut, this, std::placeholders::_1));
    _listener.support(methods::POST, std::bind(&StreamCatalogController::handlePost, this, std::placeholders::_1));
    _listener.support(methods::DEL, std::bind(&StreamCatalogController::handleDelete, this, std::placeholders::_1));
    _listener.support(methods::PATCH, std::bind(&StreamCatalogController::handlePatch, this, std::placeholders::_1));
}

}


