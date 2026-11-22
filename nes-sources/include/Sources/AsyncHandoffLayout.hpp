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

/// Runtime copy plan for an async handoff, checked when the source is constructed.
/// Input and output must have identical ordered schemas, including field names, types, and nullability.
/// Unchanged fields are copied at the same offsets; exclusively owned buffers are updated in place.
class AsyncHandoffLayout
{
public:
    struct CopiedField
    {
        size_t offset;
        size_t size;
    };

    AsyncHandoffLayout(const AsyncCallGraph& graph, const Schema<QualifiedUnboundField, Ordered>& outputSchema);

    [[nodiscard]] const Schema<QualifiedUnboundField, Ordered>& getSchema() const { return schema; }

    [[nodiscard]] const std::vector<CopiedField>& getCopiedFields() const { return copiedFields; }

    [[nodiscard]] size_t getRowSize() const { return rowSize; }

private:
    Schema<QualifiedUnboundField, Ordered> schema;
    std::vector<CopiedField> copiedFields;
    size_t rowSize = 0;
};

}
