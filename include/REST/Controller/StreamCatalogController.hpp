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

#ifndef NES_INCLUDE_REST_CONTROLLER_STREAM_CATALOG_CONTROLLER_HPP_
#define NES_INCLUDE_REST_CONTROLLER_STREAM_CATALOG_CONTROLLER_HPP_

#include <REST/Controller/BaseController.hpp>
#include <cpprest/details/basic_types.h>
#include <cpprest/http_msg.h>
#include <cpprest/json.h>

namespace NES {
class StreamCatalog;
using StreamCatalogPtr = std::shared_ptr<StreamCatalog>;

class StreamCatalogController : public BaseController {

  public:
    explicit StreamCatalogController(StreamCatalogPtr streamCatalog);

    void handleGet(const std::vector<utility::string_t>& path, web::http::http_request& request) override;
    void handlePost(const std::vector<utility::string_t>& path, web::http::http_request& message) override;
    void handleDelete(const std::vector<utility::string_t>& path, web::http::http_request& request) override;

  private:
    StreamCatalogPtr streamCatalog;
};
using StreamCatalogControllerPtr = std::shared_ptr<StreamCatalogController>;

}// namespace NES

#endif// NES_INCLUDE_REST_CONTROLLER_STREAM_CATALOG_CONTROLLER_HPP_
