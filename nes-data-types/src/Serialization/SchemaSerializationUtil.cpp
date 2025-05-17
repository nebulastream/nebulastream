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
#include <memory>
#include <API/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

SerializableSchema SchemaSerializationUtil::serializeSchema(const Schema& schema, SerializableSchema* serializedSchema)
{
    NES_DEBUG("SchemaSerializationUtil:: serialize schema {}", schema.toString());
    /// serialize all field in schema
    for (const auto& field : schema)
    {
        auto* serializedField = serializedSchema->add_fields();
        serializedField->set_name(field.getName());
        /// serialize data type
        DataTypeSerializationUtil::serializeDataType(field.getDataType(), serializedField->mutable_type());
    }

    /// Serialize layoutType
    if (schema.getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        serializedSchema->set_layouttype(SerializableSchema_MemoryLayoutType_ROW_LAYOUT);
    }
    else if (schema.getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
    {
        serializedSchema->set_layouttype(SerializableSchema_MemoryLayoutType_COL_LAYOUT);
    }

    return SerializableSchema(*serializedSchema);
}

Schema SchemaSerializationUtil::deserializeSchema(const SerializableSchema& serializedSchema)
{
    /// de-serialize field from serialized schema to the schema object.
    auto deserializedSchema = Schema();
    for (const auto& serializedField : serializedSchema.fields())
    {
        const auto& fieldName = serializedField.name();
        /// de-serialize data type
        auto type = DataTypeSerializationUtil::deserializeDataType(serializedField.type());
        deserializedSchema.addField(fieldName, type);
    }

    /// Deserialize layoutType
    switch (serializedSchema.layouttype())
    {
        case SerializableSchema_MemoryLayoutType_ROW_LAYOUT: {
            deserializedSchema.setLayoutType(Schema::MemoryLayoutType::ROW_LAYOUT);
            break;
        }
        case SerializableSchema_MemoryLayoutType_COL_LAYOUT: {
            deserializedSchema.setLayoutType(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
            break;
        }
        default: {
            NES_ERROR("SchemaSerializationUtil:: Wrong memory layout in serialization format");
        }
    }
    return deserializedSchema;
}
}
