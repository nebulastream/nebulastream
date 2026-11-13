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
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
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

/// The non-equi part of a hash join's function (e.g. `bTime >= aTime` in `id = auction AND bTime >= aTime`), evaluated on every pair whose
/// keys match. Only the fields it reads are copied into the record it is evaluated on.
struct HJResidual
{
    PhysicalFunction function;
    std::vector<Record::RecordFieldIdentifier> leftFieldNames;
    std::vector<Record::RecordFieldIdentifier> rightFieldNames;
};

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
        std::optional<HJResidual> residual);

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

    /// True if the residual holds for the pair of a left and a right record, or if there is no residual.
    /// NULL counts as false, as in the NLJ.
    nautilus::val<bool> residualHolds(
        const Record& leftRecord,
        const Record& rightRecord,
        ExecutionContext& executionCtx,
        const nautilus::val<Timestamp>& windowStart,
        const nautilus::val<Timestamp>& windowEnd) const;

    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout, rightTupleLayout;
    ChainedHashMapConfig leftHashMapConfig, rightHashMapConfig;
    std::optional<HJResidual> residual;
};

}
