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

#include <optional>
#include <string>
#include <vector>

#include <cstdint>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include "CompilationContext.hpp"
#include "Nautilus/Interface/RecordBuffer.hpp"
#include "Runtime/Execution/OperatorHandler.hpp"

namespace NES
{

/// Physical operator that serializes each input record to a binary row and appends it to a file via an operator handler.
/// The record is then forwarded to its child unchanged.
class StorePhysicalOperator final : public PhysicalOperatorConcept
{
public:
    StorePhysicalOperator(OperatorHandlerId handlerId, const Schema& inputSchema);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    void encodeAndAppend(Record& record, ExecutionContext& executionCtx) const;

    OperatorHandlerId handlerId;
    Schema inputSchema;
    std::vector<std::string> fieldNames;
    std::vector<DataType> fieldTypes;
    std::vector<uint32_t> fieldSizes;
    std::vector<uint32_t> fieldOffsets;
    uint32_t rowWidth{0};

    std::optional<PhysicalOperator> child;
};

}
