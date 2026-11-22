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

#include <Sources/AsyncHandoffLayout.hpp>

#include <optional>
#include <unordered_set>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncCallGraph.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AsyncHandoffLayout::AsyncHandoffLayout(const AsyncCallGraph& graph, const Schema<QualifiedUnboundField, Ordered>& outputSchema)
    : schema(outputSchema)
{
    PRECONDITION(
        schema == graph.getInputSchema(), "Async handoff input and output must have identical ordered schemas (field names and types)");
    PRECONDITION(schema.getUniqueFieldNames().size() == schema.size(), "Duplicate handed-off field in async schema");

    std::unordered_set<QualifiedIdentifier> producedFields;
    for (const auto& output : graph.getOutputs())
    {
        const auto field = schema.getFieldByName(output.field);
        PRECONDITION(
            field.has_value() && field->getFullyQualifiedName() == output.field,
            "Async output field {} is not in the handoff schema",
            output.field);
        PRECONDITION(
            field->getDataType() == output.result.getDataType(),
            "Async output field {} has type {}, expected {}",
            output.field,
            field->getDataType(),
            output.result.getDataType());
        producedFields.insert(output.field);
    }
    for (const auto& field : schema)
    {
        const auto size = field.getDataType().getSizeInBytesWithNull();
        if (!producedFields.contains(field.getFullyQualifiedName()))
        {
            copiedFields.push_back({.offset = rowSize, .size = size});
        }
        rowSize += size;
    }
}

}
