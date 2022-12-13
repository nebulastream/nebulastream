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

#include <GRPC/Serialization/UdfSerializationUtil.hpp>

namespace NES {

void UdfSerializationUtil::serializeJavaUdfDescriptor(const Catalogs::UDF::JavaUdfDescriptor& javaUdfDescriptor,
                                                      JavaUdfDescriptorMessage& javaUdfDescriptorMessage) {
    javaUdfDescriptorMessage.set_udf_class_name(javaUdfDescriptor.getClassName());
    javaUdfDescriptorMessage.set_udf_method_name(javaUdfDescriptor.getMethodName());
    javaUdfDescriptorMessage.set_serialized_instance(javaUdfDescriptor.getSerializedInstance().data(),
                                                     javaUdfDescriptor.getSerializedInstance().size());
    for (const auto& [className, byteCode] : javaUdfDescriptor.getByteCodeList()) {
        auto* javaClass = javaUdfDescriptorMessage.add_classes();
        javaClass->set_class_name(className);
        javaClass->set_byte_code(byteCode.data(), byteCode.size());
    }
}

}