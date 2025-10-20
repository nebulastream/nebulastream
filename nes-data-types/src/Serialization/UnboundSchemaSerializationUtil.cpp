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

#include <DataTypes/UnboundSchema.hpp>
#include <SerializableSchema.pb.h>
#include <SerializableIdentifier.pb.h>
#include <Serialization/UnboundSchemaSerializationUtl.hpp>
#include "Serialization/DataTypeSerializationUtil.hpp"
#include "Serialization/IdentifierSerializationUtil.hpp"

namespace NES
{
SerializableUnboundSchema* UnboundSchemaSerializationUtil::serializeUnboundSchema(
    const UnboundSchema& unboundSchema, SerializableUnboundSchema* serializedUnboundSchema)
{
    for (const auto& field : unboundSchema)
    {
        auto* unboundField = serializedUnboundSchema->add_fields();
        IdentifierSerializationUtil::serializeIdentifierList(field.getName(), unboundField->mutable_name());
        DataTypeSerializationUtil::serializeDataType(field.getDataType(), unboundField->mutable_type());
    }
    return serializedUnboundSchema;
}

UnboundSchema UnboundSchemaSerializationUtil::deserializeUnboundSchema(const SerializableUnboundSchema& serializedUnboundSchema)
{
    std::vector<UnboundField> fields;
    for (const auto& unboundField : serializedUnboundSchema.fields())
    {
        IdentifierList identifier = IdentifierSerializationUtil::deserializeIdentifierList(unboundField.name());
        DataType dataType = DataTypeSerializationUtil::deserializeDataType(unboundField.type());
        fields.emplace_back(identifier, dataType);
    }
    return UnboundSchema{fields};
}

}