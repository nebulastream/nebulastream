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

#include <memory>
#include <optional>
#include <vector>

#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Watermark/TimeFunction.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

class AsOfJoinInputPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    AsOfJoinInputPhysicalOperator(OperatorHandlerId operatorHandlerId, std::vector<OriginId> inputOriginIds);

    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    OperatorHandlerId operatorHandlerId;
    std::vector<OriginId> inputOriginIds;
    std::optional<PhysicalOperator> child;
};

/// A directional event-time ASOF join. Left tuples produce output after the
/// right watermark makes their predecessor selection final.
class AsOfJoinPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    AsOfJoinPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        std::shared_ptr<TupleBufferRef> leftInputBufferRef,
        std::shared_ptr<TupleBufferRef> rightInputBufferRef,
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        OriginId outputOriginId,
        std::unique_ptr<TimeFunction> leftTimeFunction,
        std::unique_ptr<TimeFunction> rightTimeFunction);

    AsOfJoinPhysicalOperator(const AsOfJoinPhysicalOperator& other);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    void processLeftRecord(ExecutionContext& executionCtx, Record& record) const;
    void processRightRecord(ExecutionContext& executionCtx, Record& record) const;
    void probeLeftRecord(ExecutionContext& executionCtx, const Record& leftRecord, const nautilus::val<Timestamp>& leftTimestamp) const;
    void releasePending(
        ExecutionContext& executionCtx, const nautilus::val<Timestamp>& rightWatermark, const nautilus::val<bool>& releaseAll) const;

    OperatorHandlerId operatorHandlerId;
    PhysicalFunction joinFunction;
    std::shared_ptr<TupleBufferRef> leftInputBufferRef;
    std::shared_ptr<TupleBufferRef> rightInputBufferRef;
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout;
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout;
    std::vector<Record::RecordFieldIdentifier> leftInputFields;
    std::vector<Record::RecordFieldIdentifier> rightInputFields;
    std::vector<Record::RecordFieldIdentifier> leftFields;
    std::vector<Record::RecordFieldIdentifier> rightFields;
    OriginId outputOriginId;
    std::unique_ptr<TimeFunction> leftTimeFunction;
    std::unique_ptr<TimeFunction> rightTimeFunction;
    std::optional<PhysicalOperator> child;
};

}
