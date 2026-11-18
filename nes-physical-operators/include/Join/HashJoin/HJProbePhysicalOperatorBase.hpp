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
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
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
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

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
        std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
        std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
        ChainedHashMapConfig leftHashMapConfig,
        ChainedHashMapConfig rightHashMapConfig,
        std::optional<PhysicalFunction> residualFunction = std::nullopt);

protected:
    /// Match-pairs probe: iterates all left hash maps against all right hash maps and emits joined records.
    /// Left hash map buffers are stored as child buffers [0, leftNumberOfHashMaps) of the record buffer, right ones follow at
    /// [leftNumberOfHashMaps, leftNumberOfHashMaps + rightNumberOfHashMaps).
    void performMatchPairsProbe(
        const BorrowedNautilusBuffer& recordBuffer,
        nautilus::val<uint64_t> leftNumberOfHashMaps,
        nautilus::val<uint64_t> rightNumberOfHashMaps,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    /// Whether a key of the entry is NULL. Such an entry only exists on the preserved side of an outer join (see
    /// HJBuildPhysicalOperator) and never matches: the hash map groups NULL keys together, but NULL = NULL is not true.
    [[nodiscard]] static nautilus::val<bool>
    keyContainsNull(const ChainedHashMapRef::ChainedEntryRef& entryRef, const ChainedHashMapConfig& hashMapConfig);

    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout, rightTupleLayout;
    ChainedHashMapConfig leftHashMapConfig, rightHashMapConfig;
    /// The hash map only matches keys. Pairs with equal keys must also satisfy this condition (the join's residual
    /// functions), if there is one; the join function itself is never evaluated.
    std::optional<PhysicalFunction> residualFunction;
};

}
