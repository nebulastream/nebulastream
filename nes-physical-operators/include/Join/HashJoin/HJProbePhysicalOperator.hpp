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
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Performs the second phase of the join. The tuples are joined via probing the previously built hash tables
class HJProbePhysicalOperator final : public StreamJoinProbePhysicalOperator
{
public:
    HJProbePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        JoinSchema joinSchema,
        std::shared_ptr<TupleBufferRef> leftBufferRef,
        std::shared_ptr<TupleBufferRef> rightBufferRef,
        HashMapOptions leftHashMapBasedOptions,
        HashMapOptions rightHashMapBasedOptions,
        JoinLogicalOperator::JoinType joinType);

    /// As the second phase gets triggered by the first phase, we receive a tuple buffer containing all information for performing the probe.
    /// Thus, we start a new pipeline and therefore, we create new Records from the built-up state.
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    /// Outer-join probe: iterates the outer (preserved) side's hash maps.
    /// For each entry, checks whether the inner side has a matching key.
    /// When emitMatchedPairs is true: matched entries produce a Cartesian product; unmatched entries emit NULL-filled records.
    /// When emitMatchedPairs is false: only unmatched entries emit NULL-filled records (used for FULL join pass 2).
    /// The emitMatchedPairs parameter is a compile-time C++ bool, NOT a nautilus::val, so dead code is eliminated
    /// before nautilus tracing, keeping each traced pipeline simple.
    template <bool EmitMatchedPairs>
    void performOuterProbe(
        nautilus::val<HashMap**> outerHashMapRefs,
        nautilus::val<uint64_t> outerNumberOfHashMaps,
        nautilus::val<HashMap**> innerHashMapRefs,
        nautilus::val<uint64_t> innerNumberOfHashMaps,
        const HashMapOptions& outerHashMapOptions,
        const HashMapOptions& innerHashMapOptions,
        const std::shared_ptr<TupleBufferRef>& outerBufferRef,
        const std::shared_ptr<TupleBufferRef>& innerBufferRef,
        const Schema& nullSideSchema,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    std::shared_ptr<TupleBufferRef> leftBufferRef, rightBufferRef;
    HashMapOptions leftHashMapOptions, rightHashMapOptions;
    JoinLogicalOperator::JoinType joinType;
};

}
