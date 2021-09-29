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

#include <REST/Controller/UdfCatalogController.hpp>
#include <Catalogs/UdfCatalog.hpp>
#include <UdfCatalogService.pb.h>
#include <Util/Logger.hpp>
#include <algorithm>
#include <iterator>

namespace NES {

using namespace std::string_literals;
using namespace Catalogs;

const std::string UdfCatalogController::path_prefix = "udf-catalog"s;

void UdfCatalogController::handlePost(const std::vector<utility::string_t>& path, http_request& request) {
    if (path[0] != UdfCatalogController::path_prefix) {
        // This is just a sanity check that the RestEngine did not call us with a wrong path.
        NES_ERROR("The RestEngine delegated an HTTP request with an unknown path prefix to the UdfCatalogController: path[0] = " << path[0]);
        internalServerErrorImpl(request);
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
        auto javaUdfDescriptor = std::make_shared<JavaUdfDescriptor>(
            javaUdfRequest.udf_class_name(), javaUdfRequest.udf_method_name(),
            serializedInstance, javaUdfByteCodeList);
        NES_DEBUG("Registering Java UDF '" << javaUdfRequest.udf_name() << "'.'");
        udfCatalog->registerJavaUdf(javaUdfRequest.udf_name(), javaUdfDescriptor);
        successMessageImpl(request, "Registered Java UDF");
    }).wait();
}

}