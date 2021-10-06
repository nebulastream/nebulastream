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
#include <REST/Controller/BaseController.hpp>
#include <unordered_set>

#include <cpprest/details/basic_types.h>
#include <cpprest/http_msg.h>

namespace NES {

namespace Catalogs {
class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;
}// namespace Catalogs

using namespace Catalogs;

class UdfCatalogController : public BaseController {

  public:
    static const std::string path_prefix;

    explicit UdfCatalogController(UdfCatalogPtr udfCatalog) : udfCatalog(std::move(udfCatalog)) {}

    void handleGet(const std::vector<utility::string_t>& path, http_request& request);

    void handlePost(const std::vector<utility::string_t>& path, http_request& request);

    void handleDelete(const std::vector<utility::string_t>& path, http_request& request);

  private:
    // Sanity check that the REST engine delegated the correct requests to this controller.
    // If this is not the case, this method constructs a InternalServerError response and returns false.
    // Handler methods should call this method in the beginning and immediately return when this method returns false.
    [[nodiscard]] static bool verifyCorrectPathPrefix(const std::string& path_prefix, http_request& request);

    // Check that a handler method was called for a known REST endpoint.
    // If this is not the case, this method constructs a BadRequest response and returns false.
    // Handler methods should call this method in the beginning and immediately return when this method returns false.
    [[nodiscard]] static bool verifyCorrectEndpoints(const std::vector<std::string>& path,
                                                     const std::unordered_set<std::string>& endpoints,
                                                     http_request& request);

    // Convenience method to check for a single known REST endpoint.
    [[nodiscard]] static bool
    verifyCorrectEndpoint(const std::vector<std::string>& path, const std::string& endpoint, http_request& request);

    // Extract the udfName parameter from the URL query string.
    // This method checks if the udfName parameter exists and if it's the only parameter.
    // If either is not true, this method constructs a BadRequest response and returns false as the first return value.
    // Otherwise, the UDF name is returned as the second return value;
    // Handler methods may call this method in the beginning and should immediately return when this method returns false.
    [[nodiscard]] static std::pair<bool, const std::string> extractUdfNameParameter(http_request& request);

    void handleGetUdfDescriptor(http_request& request);

    void handleListUdfs(http_request& request);

    UdfCatalogPtr udfCatalog;
};

}// namespace NES