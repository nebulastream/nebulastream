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

#ifndef NES_INCLUDE_GRPC_SERIALIZATION_SCHEMA_SERIALIZATION_UTIL_HPP_
#define NES_INCLUDE_GRPC_SERIALIZATION_SCHEMA_SERIALIZATION_UTIL_HPP_

#include <memory>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class SerializableSchema;
using SerializableSchemaPtr = std::shared_ptr<SerializableSchema>;

/**
 * @brief The SchemaSerializationUtil offers functionality to serialize and de-serialize schemas to the
 * corresponding protobuffer object.
 */
class SchemaSerializationUtil {
  public:
    /**
     * @brief Serializes a schema and all its fields to a SerializableSchema object.
     * @param schema SchemaPtr.
     * @param serializedSchema The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializedSchema
     */
    static SerializableSchemaPtr serializeSchema(const SchemaPtr& schema, SerializableSchema* serializedSchema);

    /**
    * @brief De-serializes the SerializableSchema and all its fields to a SchemaPtr
    * @param serializedSchema the serialized schema.
    * @return SchemaPtr
    */
    static SchemaPtr deserializeSchema(SerializableSchema* serializedSchema);
};
}// namespace NES

#endif  // NES_INCLUDE_GRPC_SERIALIZATION_SCHEMA_SERIALIZATION_UTIL_HPP_
