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

bool UdfCatalogController::verifyCorrectEndpoints(const std::vector<std::string>& path,
                                                  const std::unordered_set<std::string>& endpoints,
                                                  http_request& request) {
    if (path.size() != 2 || endpoints.find(path[1]) == endpoints.end()) {
        std::stringstream ss;
        ss << "HTTP request with unknown path: /";
        std::copy(path.begin(), path.end(), std::ostream_iterator<std::string>(ss, "/"));
        NES_WARNING(ss.str());
        badRequestImpl(request, ss.str());
        return false;
    }
    return true;
}

bool UdfCatalogController::verifyCorrectEndpoint(const std::vector<std::string>& path,
                                                 const std::string& endpoint,
                                                 http_request& request) {
    return verifyCorrectEndpoints(path, {endpoint}, request);
}

std::pair<bool, const std::string> UdfCatalogController::extractUdfNameParameter(http_request& request) {
    auto queries = web::uri::split_query(request.request_uri().query());
    auto query = queries.find("udfName");
    // Verify that the udfName parameter exists.
    if (query == queries.end()) {
        NES_DEBUG("Request does not contain the udfName parameter");
        badRequestImpl(request, "udfName parameter is missing"s);
        return {false, ""};
    }
    // Make sure that the URL contains only the udfName parameter and no others.
    if (queries.size() != 1) {
        auto unknownParameters =
            std::accumulate(queries.begin(), queries.end(), ""s, [&](std::string tmpString, const decltype(queries)::value_type& parameter) {
                const auto& key = parameter.first;
                if (key != "udfName") {
                    return (tmpString.empty() ? tmpString : tmpString + ", ") + key;
                }
                return tmpString;
            });
        NES_DEBUG("Request contains unknown parameters: " << unknownParameters);
        badRequestImpl(request, "Request contains unknown parameters: "s + unknownParameters);
        return {false, ""};
    }
    return {true, query->second};
}

void UdfCatalogController::handleGetUdfDescriptor(http_request& request) {
    auto [found, udfName] = extractUdfNameParameter(request);
    if (!found) {
        return;
    }
    auto udfDescriptor = udfCatalog->getUdfDescriptor(udfName);
    GetJavaUdfDescriptorResponse response;
    if (udfDescriptor == nullptr) {
        // Signal that the UDF does not exist in the catalog.
        NES_DEBUG("REST client tried retrieving UDF descriptor for non-existing Java UDF: " << udfName);
        response.set_found(false);
    } else {
        // Return the UDF descriptor to the client.
        NES_DEBUG("Returning UDF descriptor to REST client for Java UDF: " << udfName);
        response.set_found(true);
        auto* descriptorMessage = response.mutable_java_udf_descriptor();
        descriptorMessage->set_udf_class_name(udfDescriptor->getClassName());
        descriptorMessage->set_udf_method_name(udfDescriptor->getMethodName());
        descriptorMessage->set_serialized_instance(udfDescriptor->getSerializedInstance().data(),
                                                   udfDescriptor->getSerializedInstance().size());
        for (const auto& [className, byteCode] : udfDescriptor->getByteCodeList()) {
            auto* javaClass = descriptorMessage->add_classes();
            javaClass->set_class_name(className);
            javaClass->set_byte_code(byteCode.data(), byteCode.size());
        }
    }
    successMessageImpl(request, response.SerializeAsString());
}

void UdfCatalogController::handleListUdfs(http_request& request) {
    web::json::value result;
    // Initialize empty list of UDFs.
    result["udfs"] = web::json::value::array();
    unsigned i = 0;
    for (const auto& udf : udfCatalog->listUdfs()) {
        result["udfs"][i++] = web::json::value(udf);
    }
    successMessageImpl(request, result);
}

void UdfCatalogController::handleGet(const std::vector<utility::string_t>& path, http_request& request) {
    if (!verifyCorrectPathPrefix(path[0], request) ||
        !verifyCorrectEndpoints(path, {"getUdfDescriptor", "listUdfs"}, request)) {
        return;
    }
    if (path[1] == "getUdfDescriptor") {
        handleGetUdfDescriptor(request);
    } else if (path[1] == "listUdfs") {
        handleListUdfs(request);
    } else {
        // We should never be here because we've already checked the possible endpoints.
        NES_THROW_RUNTIME_ERROR("Illegal state.");
    }
}

void UdfCatalogController::handlePost(const std::vector<utility::string_t>& path, http_request& request) {
    if (!verifyCorrectPathPrefix(path[0], request) ||
        !verifyCorrectEndpoint(path, "registerJavaUdf", request)) {
        return;
    }
    auto udfCatalog = this->udfCatalog;
    request.extract_string(true).then([udfCatalog, &request](const utility::string_t& body) {
        // Convert protobuf message contents to JavaUdfDescriptor.
        NES_DEBUG("Parsing Java UDF descriptor from REST request");
        auto javaUdfRequest = RegisterJavaUdfRequest {};
        javaUdfRequest.ParseFromString(body);
        auto descriptorMessage = javaUdfRequest.java_udf_descriptor();
        // C++ represents the bytes type of serialized_instance and byte_code as std::strings
        // which have to be converted to typed byte arrays.
        auto serializedInstance = JavaSerializedInstance {
            descriptorMessage.serialized_instance().begin(),
            descriptorMessage.serialized_instance().end()
        };
        auto javaUdfByteCodeList = JavaUdfByteCodeList {};
        javaUdfByteCodeList.reserve(descriptorMessage.classes().size());
        for (const auto& classDefinition : descriptorMessage.classes()) {
            javaUdfByteCodeList.insert({classDefinition.class_name(),
                                        JavaByteCode{classDefinition.byte_code().begin(),
                                                     classDefinition.byte_code().end()}});
        }
        // Register JavaUdfDescriptor in UDF catalog and return success.
        try {
            auto javaUdfDescriptor = JavaUdfDescriptor::create(
                descriptorMessage.udf_class_name(), descriptorMessage.udf_method_name(),
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
    if (!verifyCorrectPathPrefix(path[0], request) ||
        !verifyCorrectEndpoint(path, "removeUdf", request)) {
        return;
    }
    auto [found, udfName] = extractUdfNameParameter(request);
    if (!found) {
        return;
    }
    NES_DEBUG("Removing Java UDF '" << udfName << "'");
    auto removed = udfCatalog->removeUdf(udfName);
    web::json::value result;
    result["removed"] = web::json::value(removed);
    successMessageImpl(request, result);
}

}