#pragma once

#include <cpprest/http_msg.h>

using namespace web;
using namespace http;

namespace NES {

   /*!
    * Dispatcher class represents the basic interface for a 
    * web serivce handler.
    */
    class BaseController {
    public: 
        void handleGet(std::vector<utility::string_t> path, http_request message);
        void handlePut(std::vector<utility::string_t> path, http_request message);
        void handlePost(std::vector<utility::string_t> path, http_request message);
        void handleDelete(std::vector<utility::string_t> path, http_request message);
        void handlePatch(std::vector<utility::string_t> path, http_request message);
        void handleHead(std::vector<utility::string_t> path, http_request message);
        void handleTrace(std::vector<utility::string_t> path, http_request message);
        void handleMerge(std::vector<utility::string_t> path, http_request message);

        void handleOptions(http_request message);

        json::value responseNotImpl(const http::method& method, utility::string_t path);
        void internalServerErrorImpl(web::http::http_request message) const;
        void successMessageImpl(const web::http::http_request& message, const web::json::value& result) const;
        void resourceNotFoundImpl(const web::http::http_request& message) const;
        utility::string_t getPath(http_request &message);
    };
}