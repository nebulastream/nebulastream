#include "REST/Controller/BaseController.hpp"

namespace NES {

void BaseController::handleDelete(std::vector<utility::string_t> path, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL, getPath(message)));
}

void BaseController::handleGet(std::vector<utility::string_t> path, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::GET, getPath(message)));
}

void BaseController::handleHead(std::vector<utility::string_t> path, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD, getPath(message)));
}

void BaseController::handleMerge(std::vector<utility::string_t> path, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE, getPath(message)));
}

void BaseController::handleTrace(std::vector<utility::string_t> path, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::TRCE, getPath(message)));
}

void BaseController::handleOptions(http_request message) {
    http_response response(status_codes::OK);
    response.headers().add(U("Allow"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Methods"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    message.reply(response);
}

json::value BaseController::responseNotImpl(const http::method& method, utility::string_t path) {
    auto response = json::value::object();
    response["path"] = json::value::string(path);
    response["http_method"] = json::value::string(method);
    return response;
}

void BaseController::successMessageImpl(const http_request& message, const json::value& result) const {
    http_response response(status_codes::OK);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    response.set_body(result);
    message.reply(response);
}

void BaseController::internalServerErrorImpl(http_request message) const {
    http_response response(status_codes::InternalError);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    message.reply(response);
}

void BaseController::resourceNotFoundImpl(const http_request& message) const {
    http_response response(status_codes::NotFound);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    message.reply(response);
}

utility::string_t BaseController::getPath(http_request& message) {
    return web::uri::decode(message.relative_uri().path());
}

}// namespace NES
