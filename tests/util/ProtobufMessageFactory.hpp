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

#include <Catalogs/UdfCatalog.hpp>
#include <UdfCatalogService.pb.h>

namespace NES {

/**
 * @brief Factory for Protobuf messages used in test code.
 */
class ProtobufMessageFactory {
  public:
    /**
     * @brief Construct a RegisterJavaUdfRequest protobuf message.
     * @see UdfCatalog::registerJavaUdf
     */
    static RegisterJavaUdfRequest createRegisterJavaUdfRequest(const std::string& udfName,
                                                               const std::string& udfClassName,
                                                               const std::string& methodName,
                                                               const JavaSerializedInstance& serializedInstance,
                                                               const JavaUdfByteCodeList& byteCodeList) {
        auto request = RegisterJavaUdfRequest{};
        request.set_udf_name(udfName);
        auto* udfDescriptor = request.mutable_java_udf_descriptor();
        udfDescriptor->set_udf_class_name(udfClassName);
        udfDescriptor->set_udf_method_name(methodName);
        udfDescriptor->set_serialized_instance(serializedInstance.data(), serializedInstance.size());
        for (const auto& [className, byteCode] : byteCodeList) {
            auto* javaClass = udfDescriptor->add_classes();
            javaClass->set_class_name(className);
            javaClass->set_byte_code(std::string{byteCode.begin(), byteCode.end()});
        }
        return request;
    }
};

} // namespace NES