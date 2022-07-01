/*
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

#ifndef NES_INCLUDE_REST_CONTROLLER_BASECONTROLLER_HPP_
#define NES_INCLUDE_REST_CONTROLLER_BASECONTROLLER_HPP_

#include <REST/CpprestForwardedRefs.hpp>
#include <cpprest/http_msg.h>
#include <map>
#include <string>
#include <vector>

namespace NES {

/*!
 * QueryManager class represents the basic interface for a
 * web service handler.
 */
class BaseController {
  public:
    virtual ~BaseController() = default;
    /**
     * @brief Handle the get request from the user
     * @param path : the resource path the user wanted to get
     * @param request : the message from the user
     */
    virtual void handleGet(const std::vector<utility::string_t>& path, web::http::http_request& request);

    /**
     * @brief Handle the put request from the user
     * @param path : the resource path the user wanted to get
     * @param request : the message from the user
     */
    virtual void handlePut(const std::vector<utility::string_t>& path, web::http::http_request& request);

    /**
     * @brief Handle the post request from the user
     * @param path : the resource path the user wanted to get
     * @param request : the message from the user
     */
    virtual void handlePost(const std::vector<utility::string_t>& path, web::http::http_request& request);

    /**
     * @brief Handle the delete request from the user
     * @param path : the resource path the user wanted to get
     * @param request : the message from the user
     */
    virtual void handleDelete(const std::vector<utility::string_t>& path, web::http::http_request& request);

    /**
     * @brief Handle the patch request from the user
     * @param path : the resource path the user wanted to get
     * @param request : the message from the user
     */
    virtual void handlePatch(const std::vector<utility::string_t>& path, web::http::http_request& request);

    /**
     * @brief Handle the head request from the user
     * @param path : the resource path the user wanted to get
     * @param request : the message from the user
     */
    virtual void handleHead(const std::vector<utility::string_t>& path, web::http::http_request& request);

    /**
     * @brief Handle trace request from the user
     * @param path : the resource path the user wanted to get
     * @param request : the message from the user
     */
    virtual void handleTrace(const std::vector<utility::string_t>& path, web::http::http_request& request);

    /**
     * @brief Handle unionWith request from the user
     * @param path : the resource path the user wanted to get
     * @param request : the message from the user
     */
    virtual void handleMerge(const std::vector<utility::string_t>& path, web::http::http_request& request);

    /**
     * @brief set http response options
     * @param request : the message from the user
     */
    static void handleOptions(const web::http::http_request& request);

    static void internalServerErrorImpl(const web::http::http_request& message);

    static void resourceNotFoundImpl(const web::http::http_request& message);
    static void noContentImpl(const web::http::http_request& message);

    /**
     * @brief Used to create and return a http_response to a user request.
     * @tparam T the type of result message. Supported types are std::string and web::json::value.
     * @param request The HTTP request
     * @param result a detailed messsage
     * @param statusCode  2XX status code
     */
    template<typename T,
             std::enable_if_t<std::is_same<T, utility::string_t>::value || std::is_same<T, web::json::value>::value, bool> = true>
    static void successMessageImpl(const web::http::http_request& request,
                                   const T& result,
                                   const web::http::status_code& statusCode = web::http::status_codes::OK) {
        web::http::http_response response(statusCode);
        response.headers().add("Access-Control-Allow-Origin", "*");
        response.headers().add("Access-Control-Allow-Headers", "Content-Type");
        response.set_body(result);
        request.reply(response);
    }

    /**
     * @brief Return a 4XX response with a detailed error message.
     * @tparam T The type of the error message. Supported types are std::string and web::json::value.
     * @param request The HTTP request.
     * @param detail The detailed error message.
     * @param statusCode 4XX status code
     */
    template<typename T,
             std::enable_if_t<std::is_same<T, utility::string_t>::value || std::is_same<T, web::json::value>::value, bool> = true>
    static void errorMessageImpl(const web::http::http_request& request,
                                 const T& detail,
                                 const web::http::status_code& statusCode = web::http::status_codes::BadRequest) {
        // Returns error with a specific error code
        web::http::http_response response(statusCode);
        response.headers().add("Access-Control-Allow-Origin", "*");
        response.headers().add("Access-Control-Allow-Headers", "Content-Type");

        // Inform REST users with reason of the error
        response.set_body(detail);
        request.reply(response);
    }

    virtual void handleException(const web::http::http_request& message, const std::exception& exc);

    /**
     * @brief Get the parameters from the request if any
     * @param request : the user request
     * @return a map containing parameter keys and values
     */
    static std::map<utility::string_t, utility::string_t> getParameters(web::http::http_request& request);
};
}// namespace NES
#endif// NES_INCLUDE_REST_CONTROLLER_BASECONTROLLER_HPP_
