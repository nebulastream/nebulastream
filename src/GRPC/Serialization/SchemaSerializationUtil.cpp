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

#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Logger.hpp>

namespace NES {

SerializableSchemaPtr SchemaSerializationUtil::serializeSchema(const SchemaPtr& schema, SerializableSchema* serializedSchema) {
    NES_DEBUG("SchemaSerializationUtil:: serialize schema " << schema->toString());
    // serialize all field in schema
    for (const auto& field : schema->fields) {
        auto* serializedField = serializedSchema->add_fields();
        serializedField->set_name(field->getName());
        // serialize data type
        DataTypeSerializationUtil::serializeDataType(field->getDataType(), serializedField->mutable_type());
    }

    // Serialize layoutType
    if (schema->layoutType == Schema::ROW_LAYOUT) {
        serializedSchema->set_layouttype(SerializableSchema_MemoryLayoutType_ROW_LAYOUT);
    } else if (schema->layoutType == Schema::COL_LAYOUT) {
        serializedSchema->set_layouttype(SerializableSchema_MemoryLayoutType_COL_LAYOUT);
    }

    return std::make_shared<SerializableSchema>(*serializedSchema);
}

SchemaPtr SchemaSerializationUtil::deserializeSchema(SerializableSchema* serializedSchema) {
    // de-serialize field from serialized schema to the schema object.
    NES_DEBUG("SchemaSerializationUtil:: deserialize schema ");
    auto deserializedSchema = Schema::create();
    for (auto serializedField : serializedSchema->fields()) {
        auto fieldName = serializedField.name();
        // de-serialize data type
        auto type = DataTypeSerializationUtil::deserializeDataType(serializedField.mutable_type());
        deserializedSchema->addField(fieldName, type);
    }

    // Deserialize layoutType
    deserializedSchema->layoutType = (Schema::MemoryLayoutType)(serializedSchema->layouttype());

    return deserializedSchema;
}

}// namespace NES