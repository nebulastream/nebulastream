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

#include <Catalogs/UdfCatalog.hpp>
#include <Exceptions/UdfException.hpp>
#include <REST/Controller/UdfCatalogController.hpp>
#include <Util/Logger.hpp>
#include <UdfCatalogService.pb.h>
#include <algorithm>
#include <iterator>

namespace NES {

using namespace std::string_literals;
using namespace Catalogs;

const std::string UdfCatalogController::path_prefix = "udf-catalog"s;

bool UdfCatalogController::verifyCorrectPathPrefix(const std::string& path_prefix, http_request& request) {
    if (path_prefix != UdfCatalogController::path_prefix) {
        NES_ERROR("The RestEngine delegated an HTTP request with an unknown path prefix to the UdfCatalogController: path[0] = " << path_prefix);
        internalServerErrorImpl(request);
        return false;
    }
    return true;
}

void UdfCatalogController::handlePost(const std::vector<utility::string_t>& path, http_request& request) {
    if (!verifyCorrectPathPrefix(path[0], request)) {
        return;
    }
    if (path.size() != 2 || path[1] != "registerJavaUdf") {
        std::stringstream ss;
        ss << "HTTP request with unknown path: /";
        std::copy(path.begin(), path.end(), std::ostream_iterator<std::string>(ss, "/"));
        NES_WARNING(ss.str());
        badRequestImpl(request, ss.str());
        return;
    }
    auto udfCatalog = this->udfCatalog;
    request.extract_string(true).then([udfCatalog, &request](const utility::string_t& body) {
        // Convert protobuf message contents to JavaUdfDescriptor.
        NES_DEBUG("Parsing Java UDF descriptor from REST request");
        auto javaUdfRequest = RegisterJavaUdfRequest {};
        javaUdfRequest.ParseFromString(body);
        // C++ represents the bytes type of serialized_instance and byte_code as std::strings
        // which have to be converted to typed byte arrays.
        auto serializedInstance = JavaSerializedInstance {
            javaUdfRequest.serialized_instance().begin(), javaUdfRequest.serialized_instance().end()
        };
        auto javaUdfByteCodeList = JavaUdfByteCodeList {};
        javaUdfByteCodeList.reserve(javaUdfRequest.classes().size());
        for (const auto& classDefinition : javaUdfRequest.classes()) {
            javaUdfByteCodeList.insert({classDefinition.class_name(),
                                        JavaByteCode{classDefinition.byte_code().begin(),
                                                     classDefinition.byte_code().end()}});
        }
        // Register JavaUdfDescriptor in UDF catalog and return success.
        try {
            auto javaUdfDescriptor = std::make_shared<JavaUdfDescriptor>(
                javaUdfRequest.udf_class_name(), javaUdfRequest.udf_method_name(),
                serializedInstance, javaUdfByteCodeList);
            NES_DEBUG("Registering Java UDF '" << javaUdfRequest.udf_name() << "'.'");
            udfCatalog->registerJavaUdf(javaUdfRequest.udf_name(), javaUdfDescriptor);
        } catch (const UdfException& e) {
            NES_WARNING("Exception occurred during UDF registration: " << e.what());
            // Just return the exception message to the client, not the stack trace.
            badRequestImpl(request, e.getMessage());
            return;
        }
        successMessageImpl(request, "Registered Java UDF");
    }).wait();
}

void UdfCatalogController::handleDelete(const std::vector<utility::string_t>& path, http_request& request) {
    if (!verifyCorrectPathPrefix(path[0], request)) {
        return;
    }
    auto queries = web::uri::split_query(request.request_uri().query());
    auto query = queries.find("udfName");
    if (query == queries.end()) {
        badRequestImpl(request, "udfName parameter is missing"s);
        return;
    }
    // Make sure that the URL contains only the udfName parameter and no others.
    if (queries.size() != 1) {
        auto unknownParameters =
            std::accumulate(queries.begin(), queries.end(), ""s, [&](std::string s, const decltype(queries)::value_type& parameter) {
                const auto& [key, _] = parameter;
                if (key != "udfName") {
                    return (s.empty() ? s + ", " : s) + key;
                }
                return s;
            });
        NES_DEBUG("Request contains unknown parameters: " << unknownParameters);
        badRequestImpl(request, "Request contains unknown parameters"s + unknownParameters);
        return;
    }
    auto udfName = query->second;
    NES_DEBUG("Removing Java UDF '" << udfName << "'");
    auto removed = udfCatalog->removeUdf(udfName);
    web::json::value result;
    result["removed"] = web::json::value(removed);
    successMessageImpl(request, result);
}

}