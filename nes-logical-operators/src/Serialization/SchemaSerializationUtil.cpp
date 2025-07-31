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
#include <cstdint>
#include <unordered_map>

#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include "Serialization/IdentifierSerializationUtil.hpp"

#include <SerializableSchema.pb.h>
#include <SerializableVariantDescriptor.pb.h>
#include <SerializableIdentifier.pb.h>

namespace NES
{

SerializableSchema* SchemaSerializationUtil::serializeSchema(const Schema& schema, SerializableSchema* serializedSchema)
{
    NES_DEBUG("SchemaSerializationUtil:: serialize schema {}", schema);
    /// serialize all field in schema
    for (const auto& field : schema)
    {
        auto* serializedField = serializedSchema->add_fields();
        IdentifierSerializationUtil::serializeIdentifier(field.getLastName(), serializedField->mutable_name());
        DataTypeSerializationUtil::serializeDataType(field.getDataType(), serializedField->mutable_type());
        serializedField->set_operator_id(field.getProducedBy().getId().getRawValue());
    }

    return serializedSchema;
}

Schema SchemaSerializationUtil::deserializeSchema(const SerializableSchema& serializedSchema, const std::vector<LogicalOperator>& operators)
{
    std::unordered_map<uint64_t, LogicalOperator> operatorMap;
    for (const auto& oper : operators)
    {
        operatorMap.emplace(oper.getId().getRawValue(), oper);
    }
    /// de-serialize field from serialized schema to the schema object.
    NES_DEBUG("SchemaSerializationUtil:: deserialize schema ");
    std::vector<Field> fields;
    for (const auto& serializedField : serializedSchema.fields())
    {
        auto foundOperator = operatorMap.find(serializedField.operator_id());
        if (foundOperator == operatorMap.end())
        {
            throw CannotDeserialize("Did not find operator with id {} in operators list", serializedField.operator_id());
        }
        const auto fieldName = IdentifierSerializationUtil::deserializeIdentifier(serializedField.name());
        /// de-serialize data type
        auto type = DataTypeSerializationUtil::deserializeDataType(serializedField.type());
        fields.emplace_back(foundOperator->second, fieldName, type);
    }
    return Schema{fields};
}
}
