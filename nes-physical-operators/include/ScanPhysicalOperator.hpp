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

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// @brief This basic scan operator extracts records from a base tuple buffer according to a memory layout.
/// Furthermore, it supports projection push down to eliminate unneeded reads
class ScanPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    explicit ScanPhysicalOperator(
        std::shared_ptr<TupleBufferRef> bufferRef,
        std::vector<Record::RecordFieldIdentifier> projections,
        uint64_t runtimeInputFormatterSlot = 1);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    [[nodiscard]] std::shared_ptr<TupleBufferRef> getBufferRef() const;
    [[nodiscard]] uint64_t getRuntimeInputFormatterSlot() const;
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    std::shared_ptr<TupleBufferRef> bufferRef;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::optional<PhysicalOperator> child;
    bool isRawScan = false;
    uint64_t runtimeInputFormatterSlot = 1;

    void rawScan(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
};

}
