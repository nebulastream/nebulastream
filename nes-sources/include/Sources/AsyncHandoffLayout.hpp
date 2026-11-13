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

#include <cstddef>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/AsyncCallGraph.hpp>

namespace NES
{

/// One layout contract for both halves of a planner-created async handoff.
/// Construction requires identical field sets/types; order may differ. Only fields not produced by the graph are copied.
/// The planner constructs this before creating either descriptor; the worker reconstructs the same offsets locally.
class AsyncHandoffLayout
{
public:
    struct CopiedField
    {
        size_t inputOffset;
        size_t outputOffset;
        size_t size;
    };

    AsyncHandoffLayout(const AsyncCallGraph& graph, Schema<QualifiedUnboundField, Ordered> outputSchema);

    [[nodiscard]] const Schema<QualifiedUnboundField, Ordered>& getInputSchema() const { return inputSchema; }

    [[nodiscard]] const Schema<QualifiedUnboundField, Ordered>& getOutputSchema() const { return outputSchema; }

    [[nodiscard]] const std::vector<CopiedField>& getCopiedFields() const { return copiedFields; }

    [[nodiscard]] size_t getInputRowSize() const { return inputRowSize; }

    [[nodiscard]] size_t getOutputRowSize() const { return outputRowSize; }

    [[nodiscard]] bool matchesInputLayout() const { return matchingLayouts; }

private:
    Schema<QualifiedUnboundField, Ordered> inputSchema;
    Schema<QualifiedUnboundField, Ordered> outputSchema;
    std::vector<CopiedField> copiedFields;
    size_t inputRowSize = 0;
    size_t outputRowSize = 0;
    bool matchingLayouts = true;
};

}
