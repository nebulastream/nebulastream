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
#include <Functions/PhysicalFunction.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Shared base for all hash join probe operators (inner, outer).
/// Holds the HJ-specific state (buffer refs, hash map options) and the match-pairs probe logic.
/// This avoids duplicating the match-pairs iteration across probe variants while keeping inner and outer
/// as independent siblings, neither inherits from the other.
class HJProbePhysicalOperatorBase : public StreamJoinProbePhysicalOperator
{
public:
    HJProbePhysicalOperatorBase(
        OperatorHandlerId operatorHandlerId,
        PhysicalFunction joinFunction,
        WindowMetaData windowMetaData,
        JoinSchema joinSchema,
        std::shared_ptr<TupleBufferRef> leftBufferRef,
        std::shared_ptr<TupleBufferRef> rightBufferRef,
        HashMapOptions leftHashMapOptions,
        HashMapOptions rightHashMapOptions);

protected:
    /// Match-pairs probe: iterates all left hash maps against all right hash maps and emits joined records
    void performMatchPairsProbe(
        nautilus::val<HashMap**> leftHashMapRefs,
        nautilus::val<uint64_t> leftNumberOfHashMaps,
        nautilus::val<HashMap**> rightHashMapRefs,
        nautilus::val<uint64_t> rightNumberOfHashMaps,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    std::shared_ptr<TupleBufferRef> leftBufferRef, rightBufferRef;
    HashMapOptions leftHashMapOptions, rightHashMapOptions;
};

}
