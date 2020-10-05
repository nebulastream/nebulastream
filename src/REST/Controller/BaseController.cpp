#include "REST/Controller/BaseController.hpp"

namespace NES {

void BaseController::handleDelete(std::vector<utility::string_t>, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL, getPath(message)));
}

void BaseController::handleGet(std::vector<utility::string_t>, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::GET, getPath(message)));
}

void BaseController::handleHead(std::vector<utility::string_t>, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD, getPath(message)));
}

void BaseController::handleMerge(std::vector<utility::string_t>, http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE, getPath(message)));
}

void BaseController::handleTrace(std::vector<utility::string_t>, http_request message) {
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

void BaseController::successMessageImpl(const http_request& message, const utf8string& result) const {
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

void BaseController::noContentImpl(const http_request& message) const {
    // Return response with http code 204 to indicate no content is returned
    http_response response(status_codes::NoContent);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    message.reply(response);
}

void BaseController::badRequestImpl(const web::http::http_request& message, const web::json::value& detail) const {
    // Returns error with http code 400 to indicate bad user request
    http_response response(status_codes::BadRequest);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));

    // Inform REST users with reason of the error as JSON response body
    response.set_body(detail);
    message.reply(response);
}

utility::string_t BaseController::getPath(http_request& message) {
    return web::uri::decode(message.relative_uri().path());
}

void BaseController::handleException(const web::http::http_request& message, const std::exception& exc) {
    json::value errorResponse{};
    const char* exceptionMsg = exc.what();
    const auto paths = web::uri::split_path(web::uri::decode(message.relative_uri().path()));

    if (strcmp(exceptionMsg, "Key not found") == 0) {
        // handle error caused by incomplete body request submitted by the user
        errorResponse["message"] = json::value::string("Wrong input format");

        // define required input fields based on path
        if (paths[0] == "streamCatalog") {
            if (paths[1] == "allPhysicalStream" || paths[1] == "deleteLogicalStream") {
                errorResponse["detail"] = json::value::string("Parameter streamName must be provided");
            } else if (paths[1] == "addLogicalStream" || paths[1] == "updateLogicalStream") {
                errorResponse["detail"] = json::value::string("Parameter streamName and schema must be provided");
            }
        } else if (paths[0] == "query") {
            if (paths[1] == "execution-plan" || paths[1] == "execute-query") {
                errorResponse["detail"] = json::value::string("Parameter userQuery and strategyName must be provided");
            } else if (paths[1] == "query-plan") {
                errorResponse["detail"] = json::value::string("Parameter userQuery must be provided");
            }
        } else if (paths[0] == "queryCatalog") {
            if (paths[1] == "queries") {
                errorResponse["detail"] = json::value::string("Parameter status must be provided");
            } else if (paths[1] == "query") {
                errorResponse["detail"] = json::value::string("Parameter queryId must be provided");
            }
        }
        this->badRequestImpl(message, errorResponse);

    } else if (std::string(exceptionMsg).find("Syntax error: Malformed object literal") != std::string::npos) {
        // handle error caused by syntax error in input body
        errorResponse["message"] = json::value::string("Syntax error");
        errorResponse["detail"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);

    } else if (std::string(exceptionMsg).find("Could not remove logical stream") != std::string::npos) {
        // handle error caused by failure to remove logical stream
        errorResponse["message"] = json::value::string(exceptionMsg);
        // TODO: Add possible cause of failure (stream not exists, stream attached to physical streams, etc)
        this->badRequestImpl(message, errorResponse);

    } else if (std::string(exceptionMsg).find("Unable to update logical stream") != std::string::npos) {
        // handle error caused by invalid code (query or schema) submitted by the user
        errorResponse["message"] = json::value::string(exceptionMsg);
        // TODO: Add possible cause of failure (stream not exists, stream attached to physical streams, etc)
        this->badRequestImpl(message, errorResponse);

    } else if (strcmp(exceptionMsg, "Compilation failed") == 0) {
        // handle error caused by invalid code (query or schema) submitted by the user
        errorResponse["message"] = json::value::string("Compilation failed");

        // define what was failed to be compiled based on path
        if (paths[0] == "streamCatalog") {
            if (paths[1] == "addLogicalStream" || paths[1] == "updateLogicalStream") {
                errorResponse["detail"] = json::value::string("Unable to compile the submitted schema");
            }
        } else if (paths[0] == "query") {
            if (paths[1] == "execution-plan" || paths[1] == "query-plan" || paths[1] == "execute-query") {
                errorResponse["detail"] = json::value::string("Unable to compile the submitted query");
            }
        }
        this->badRequestImpl(message, errorResponse);
    } else if (std::string(exceptionMsg).find("Required stream does not exists") != std::string::npos) {
        // handle error caused by invalid submitting a query on a stream that does not exist
        errorResponse["message"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);

    } else if (std::string(exceptionMsg).find("Unknown placement strategy name") != std::string::npos) {
        // handle error caused by invalid submitting a query using a strategy that does not exist
        errorResponse["message"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);

    } else if (std::string(exceptionMsg).find("Unknown query status") != std::string::npos) {
        // handle error caused by invalid submitting a query listing request with unknown status
        errorResponse["message"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);

    } else if (std::string(exceptionMsg).find("Could not delete query with id") != std::string::npos) {
        // handle error caused by failure in deleting a query
        errorResponse["message"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);
    } else {
        this->internalServerErrorImpl(message);
    }
}

}// namespace NES
