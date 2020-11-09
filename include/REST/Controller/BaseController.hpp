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

#pragma once

#include <cpprest/http_msg.h>

using namespace web;
using namespace http;

namespace NES {

/*!
 * QueryManager class represents the basic interface for a
 * web service handler.
 */
class BaseController {
  public:
    /**
     * @brief Handle the get request from the user
     * @param path : the resource path the user wanted to get
     * @param message : the message from the user
     */
    void handleGet(std::vector<utility::string_t> path, http_request message);

    /**
     * @brief Handle the put request from the user
     * @param path : the resource path the user wanted to get
     * @param message : the message from the user
     */
    void handlePut(std::vector<utility::string_t> path, http_request message);

    /**
     * @brief Handle the post request from the user
     * @param path : the resource path the user wanted to get
     * @param message : the message from the user
     */
    void handlePost(std::vector<utility::string_t> path, http_request message);

    /**
     * @brief Handle the delete request from the user
     * @param path : the resource path the user wanted to get
     * @param message : the message from the user
     */
    void handleDelete(std::vector<utility::string_t> path, http_request message);

    /**
     * @brief Handle the patch request from the user
     * @param path : the resource path the user wanted to get
     * @param message : the message from the user
     */
    void handlePatch(std::vector<utility::string_t> path, http_request message);

    /**
     * @brief Handle the head request from the user
     * @param path : the resource path the user wanted to get
     * @param message : the message from the user
     */
    void handleHead(std::vector<utility::string_t> path, http_request message);

    /**
     * @brief Handle trace request from the user
     * @param path : the resource path the user wanted to get
     * @param message : the message from the user
     */
    void handleTrace(std::vector<utility::string_t> path, http_request message);

    /**
     * @brief Handle merge request from the user
     * @param path : the resource path the user wanted to get
     * @param message : the message from the user
     */
    void handleMerge(std::vector<utility::string_t> path, http_request message);

    /**
     * @brief set http response options
     * @param message : the message from the user
     */
    void handleOptions(http_request message);

    json::value responseNotImpl(const http::method& method, utility::string_t path);
    void internalServerErrorImpl(web::http::http_request message) const;
    void successMessageImpl(const web::http::http_request& message, const web::json::value& result) const;
    void successMessageImpl(const web::http::http_request& message, const utf8string& result) const;

    void resourceNotFoundImpl(const web::http::http_request& message) const;
    void noContentImpl(const web::http::http_request& message) const;
    void badRequestImpl(const web::http::http_request& message, const web::json::value& detail) const;

    void handleException(const web::http::http_request& message, const std::exception& exc);
    utility::string_t getPath(http_request& message);
};
}// namespace NES