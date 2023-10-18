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

#ifndef NES_CORE_INCLUDE_GRPC_SERIALIZATION_UDFSERIALIZATIONUTIL_HPP_
#define NES_CORE_INCLUDE_GRPC_SERIALIZATION_UDFSERIALIZATIONUTIL_HPP_

#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Operators/LogicalOperators/UDFs/UDFDescriptor.hpp>
#include <JavaUdfDescriptorMessage.pb.h>

namespace NES {

/**
 * Utility class to serialize/deserialize a Java UDF descriptor.
 */
class UDFSerializationUtil {
  public:
    /**
     * Serialize a Java UDF descriptor to a protobuf message.
     * @param javaUdfDescriptor The Java UDF descriptor that should be serialized.
     * @param javaUdfDescriptorMessage A mutable protobuf message into which the Java UDF descriptor is serialized.
     */
    static void serializeJavaUDFDescriptor(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor,
                                           JavaUdfDescriptorMessage& javaUDFDescriptorMessage);

    /**
     * Deserialize a protobuf message representing a Java UDF descriptor.
     * @param javaUdfDescriptorMessage The Java UDF descriptor protobuf message.
     * @return A Java UDF descriptor that was deserialized from the protobuf message.
     */
    static Catalogs::UDF::JavaUDFDescriptorPtr
    deserializeJavaUDFDescriptor(const JavaUdfDescriptorMessage& javaUDFDescriptorMessage);
};

}// namespace NES

#endif// NES_CORE_INCLUDE_GRPC_SERIALIZATION_UDFSERIALIZATIONUTIL_HPP_
