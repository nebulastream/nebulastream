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

#include <algorithm>
#include <cstddef>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AsyncHandoffLayout::AsyncHandoffLayout(const AsyncCallGraph& graph, Schema<QualifiedUnboundField, Ordered> outputSchema)
    : inputSchema(graph.getInputSchema()), outputSchema(std::move(outputSchema))
{
    std::unordered_map<QualifiedIdentifier, std::pair<size_t, DataType>> outputFields;
    for (const auto& field : this->outputSchema)
    {
        const auto inserted = outputFields.emplace(field.getFullyQualifiedName(), std::pair{outputRowSize, field.getDataType()}).second;
        PRECONDITION(inserted, "Duplicate async output field {}", field.getFullyQualifiedName());
        outputRowSize += field.getDataType().getSizeInBytesWithNull();
    }
    PRECONDITION(inputSchema.size() == outputFields.size(), "Handed-off rows must have exactly the async source's fields");
    for (const auto& output : graph.getOutputs())
    {
        const auto field = outputFields.find(output.field);
        PRECONDITION(field != outputFields.end(), "Async output field {} is not in the handoff schema", output.field);
        PRECONDITION(
            field->second.second == output.result.getDataType(),
            "Async output field {} has type {}, expected {}",
            output.field,
            field->second.second,
            output.result.getDataType());
    }
    IF_PRECONDITION(std::unordered_set<QualifiedIdentifier> inputNames;)
    for (const auto& field : inputSchema)
    {
        const auto name = field.getFullyQualifiedName();
        PRECONDITION(inputNames.insert(name).second, "Duplicate handed-off field {}", name);
        const auto output = outputFields.find(name);
        PRECONDITION(output != outputFields.end(), "Handed-off field {} is not produced by the async source", name);
        PRECONDITION(
            field.getDataType() == output->second.second,
            "Async input field {} has type {}, expected {}",
            name,
            field.getDataType(),
            output->second.second);
        matchingLayouts = matchingLayouts && output->second.first == inputRowSize;
        const auto size = field.getDataType().getSizeInBytesWithNull();
        if (std::ranges::none_of(graph.getOutputs(), [&](const auto& result) { return result.field == name; }))
        {
            copiedFields.push_back({.inputOffset = inputRowSize, .outputOffset = output->second.first, .size = size});
        }
        inputRowSize += size;
    }
}

}
