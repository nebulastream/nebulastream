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
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// Forward declaration suffices: only named as makeChainedHashMapRef's return type here; the .cpp includes the full header.
class ChainedHashMapRef;

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
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        HashMapOptions leftHashMapOptions,
        HashMapOptions rightHashMapOptions);

protected:
    /// Pins the hash map TupleBuffer stored as the `index`-th child buffer of the record buffer that `recordBufferRef` points to.
    static OwnedNautilusBuffer pinHashMapBuffer(const nautilus::val<TupleBuffer*>& recordBufferRef, const nautilus::val<uint64_t>& index);

    /// Match-pairs probe: iterates all left hash maps against all right hash maps and emits joined records.
    /// Left hash map buffers are stored as child buffers [0, leftNumberOfHashMaps) of the record buffer, right ones follow at
    /// [leftNumberOfHashMaps, leftNumberOfHashMaps + rightNumberOfHashMaps).
    void performMatchPairsProbe(
        const nautilus::val<TupleBuffer*>& recordBufferRef,
        nautilus::val<uint64_t> leftNumberOfHashMaps,
        nautilus::val<uint64_t> rightNumberOfHashMaps,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    /// Builds a ChainedHashMapRef view over the hash map stored in `hashMapBufferRef` using the key/value layout described by `options`.
    static ChainedHashMapRef makeChainedHashMapRef(const nautilus::val<TupleBuffer*>& hashMapBufferRef, const HashMapOptions& options);

    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout, rightTupleLayout;
    HashMapOptions leftHashMapOptions, rightHashMapOptions;
};

}
