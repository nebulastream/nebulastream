/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include "REST/Controller/BaseController.hpp"
#include <REST/RestEngine.hpp>
#include <utility>

namespace NES {

void BaseController::handleGet(const std::vector<utility::string_t>&, http_request& request) {
  RestEngine::returnDefaultNotImplementedReply(methods::GET, request);
}

void BaseController::handlePut(const std::vector<utility::string_t>&, http_request& request) {
  RestEngine::returnDefaultNotImplementedReply(methods::PUT, request);
}

void BaseController::handlePost(const std::vector<utility::string_t>&, http_request& request) {
  RestEngine::returnDefaultNotImplementedReply(methods::TRCE, request);
}

void BaseController::handleDelete(const std::vector<utility::string_t>&, http_request& request) {
  RestEngine::returnDefaultNotImplementedReply(methods::DEL, request);
}

void BaseController::handlePatch(const std::vector<utility::string_t>&, http_request& request) {
  RestEngine::returnDefaultNotImplementedReply(methods::TRCE, request);
}
void BaseController::handleHead(const std::vector<utility::string_t>&, http_request& request) {
  RestEngine::returnDefaultNotImplementedReply(methods::HEAD, request);
}

void BaseController::handleTrace(const std::vector<utility::string_t>&, http_request& request) {
  RestEngine::returnDefaultNotImplementedReply(methods::TRCE, request);
}

void BaseController::handleMerge(const std::vector<utility::string_t>&, http_request& request) {
  RestEngine::returnDefaultNotImplementedReply(methods::MERGE, request);
}

void BaseController::handleOptions(const http_request& request) {
    http_response response(status_codes::OK);
    response.headers().add(U("Allow"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Methods"), U("GET, POST, OPTIONS"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    request.reply(response);
}

void BaseController::internalServerErrorImpl(const http_request& message) {
    http_response response(status_codes::InternalError);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    response.headers().add(U("Access-Control-Allow-Headers"), U("Content-Type"));
    message.reply(response);
}

void BaseController::resourceNotFoundImpl(const http_request& message) {
    http_response response(status_codes::NotFound);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    message.reply(response);
}

void BaseController::noContentImpl(const http_request& message) {
    // Return response with http code 204 to indicate no content is returned
    http_response response(status_codes::NoContent);
    response.headers().add(U("Access-Control-Allow-Origin"), U("*"));
    message.reply(response);
}

template void BaseController::badRequestImpl<std::string>(const web::http::http_request& request, const std::string& detail);
template void BaseController::badRequestImpl<web::json::value>(const web::http::http_request& request,
                                                               const web::json::value& detail);

std::map<utility::string_t, utility::string_t> BaseController::getParameters(http_request& request) {
    return uri::split_query(request.request_uri().query());
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
        } else if (paths[0] == "topology") {
            if (paths[1] == "addParent" || paths[1] == "removeParent") {
                errorResponse["detail"] = json::value::string("Parameter childId and parentId must be provided");
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
    } else if (std::string(exceptionMsg).find("SyntacticQueryValidation") != std::string::npos) {
        // handle error caused by Syntactic Query Validation
        errorResponse["message"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);
    } else if (std::string(exceptionMsg).find("SemanticQueryValidation") != std::string::npos) {
        // handle error caused by Semantic Query Validation
        errorResponse["message"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);
    } else if (std::string(exceptionMsg).find("Could not add parent for node in topology") != std::string::npos) {
        // handle error caused by failure in adding a parent for a node in topology
        errorResponse["message"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);
    } else if (std::string(exceptionMsg).find("Could not remove parent for node in topology") != std::string::npos) {
        // handle error caused by failure in removed a parent for a node in topology
        errorResponse["message"] = json::value::string(exceptionMsg);
        this->badRequestImpl(message, errorResponse);
    } else {
        this->internalServerErrorImpl(message);
    }
}

}// namespace NES
