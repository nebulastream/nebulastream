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

#pragma once

#include <memory>
#include <DataTypes/Schema.hpp>

namespace NES
{
class SerializableSchema;

/// TODO SchemaSerializationUtil should be converted to namespace
/// TODO serializeSchema should return nothing

/**
 * @brief The SchemaSerializationUtil offers functionality to serialize and de-serialize schemas to the
 * corresponding protobuffer object.
 */
class SchemaSerializationUtil
{
public:
    /**
     * @brief Serializes a schema and all its fields to a SerializableSchema object.
     * @param schema Schema.
     * @param serializedSchema The corresponding protobuff object, which is used to capture the state of the object.
     * @return the modified serializedSchema
     */
    static std::shared_ptr<SerializableSchema> serializeSchema(Schema schema, SerializableSchema* serializedSchema);

    /**
    * @brief De-serializes the SerializableSchema and all its fields to a Schema
    * @param serializedSchema the serialized schema.
    * @return Schema
    */
    static Schema deserializeSchema(const SerializableSchema& serializedSchema);
};
}
