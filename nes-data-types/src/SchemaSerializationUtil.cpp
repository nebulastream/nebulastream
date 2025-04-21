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
#include <Serialization/SchemaSerializationUtil.hpp>
#include <DataTypeSerializationFactory.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

std::shared_ptr<SerializableSchema> SchemaSerializationUtil::serializeSchema(const Schema& schema, SerializableSchema* serializedSchema)
{
    /// serialize all field in schema
    for (const auto& [identifier, type] : schema.getFields())
    {
        /// serialize data type
        auto* serializedField = serializedSchema->add_fields();
        DataTypeSerializationFactory::instance().serialize(type, *serializedField->mutable_type());
        serializeSchemaIdentifier(identifier, *serializedField->mutable_identifier());
    }

    return std::make_shared<SerializableSchema>(*serializedSchema);
}

void SchemaSerializationUtil::serializeSchemaIdentifier(const Schema::Identifier& identifier, SchemaIdentifier& serializedIdentifier)
{
    serializedIdentifier.set_name(identifier.name);
    if(identifier.table) {
        serializedIdentifier.set_table(*identifier.table);
    }
}

Schema SchemaSerializationUtil::deserializeSchema(const SerializableSchema& serializedSchema)
{
    /// de-serialize field from serialized schema to the schema object.
    Schema deserializedSchema{};
    for (const auto& serializedField : serializedSchema.fields())
    {
        deserializedSchema.addField(
            deserializeSchemaIdentifier(serializedField.identifier()),
            DataTypeSerializationFactory::instance().deserialize(serializedField.type()));
    }

    return deserializedSchema;
}
Schema::Identifier SchemaSerializationUtil::deserializeSchemaIdentifier(const SchemaIdentifier& identifier)
{
    return {identifier.name(), identifier.table()};
}
}
