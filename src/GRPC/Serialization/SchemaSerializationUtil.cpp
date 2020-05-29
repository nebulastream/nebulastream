#include <API/Schema.hpp>
#include <GRPC/Serialization/DataTypeSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <SerializableOperator.pb.h>
#include <Util/Logger.hpp>

namespace NES {

SerializableSchema* SchemaSerializationUtil::serializeSchema(SchemaPtr schema, SerializableSchema* serializedSchema) {
    NES_DEBUG("SchemaSerializationUtil:: serialize schema " << schema->toString());
    // serialize all field in schema
    for (const auto& field : schema->fields) {
        auto serializedField = serializedSchema->add_fields();
        serializedField->set_name(field->name);
        // serialize data type
        DataTypeSerializationUtil::serializeDataType(field->getDataType(), serializedField->mutable_type());
    }
    return serializedSchema;
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
    return deserializedSchema;
}

}// namespace NES