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

#include <Catalogs/UDF/JavaUDFDescriptor.hpp>
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <Catalogs/UDF/UDFDescriptor.hpp>
#include <UDFDescriptorMessage.pb.h>

namespace NES {

/**
 * Utility class to serialize/deserialize a Java UDF descriptor.
 */
class UDFSerializationUtil {
  public:
    /**
     * Serialize a UDF descriptor to a protobuf message.
     * @param udfDescriptor The Java UDF descriptor that should be serialized.
     * @param udfDescriptorMessage A mutable protobuf message into which the UDF descriptor is serialized.
     */
    static void serializeUDFDescriptor(const Catalogs::UDF::UDFDescriptorPtr& udfDescriptor,
                                           UDFDescriptorMessage& udfDescriptorMessage);

    /**
     * Deserialize a protobuf message representing a UDF descriptor.
     * @param udfDescriptorMessage The UDF descriptor protobuf message.
     * @return A UDF descriptor that was deserialized from the protobuf message.
     */
    static Catalogs::UDF::UDFDescriptorPtr
    deserializeUDFDescriptor(const UDFDescriptorMessage& udfDescriptorMessage);
};

}// namespace NES

#endif// NES_CORE_INCLUDE_GRPC_SERIALIZATION_UDFSERIALIZATIONUTIL_HPP_
