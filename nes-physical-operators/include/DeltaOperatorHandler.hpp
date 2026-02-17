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
#include <memory>
#include <unordered_map>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <folly/Synchronized.h>

namespace NES
{

/// Operator handler for the delta operator that maintains state across tuple buffers.
/// Uses a three-way handshake protocol to handle out-of-order buffer processing:
/// - Each buffer stores its last delta field values when it finishes (close).
/// - If a buffer's predecessor hasn't finished yet, the buffer stores its first record
///   so the predecessor can emit the deferred delta when it finishes.
/// - If the predecessor already finished before the buffer's first record is processed,
///   the record is resolved immediately.
class DeltaOperatorHandler final : public OperatorHandler
{
public:
    DeltaOperatorHandler(size_t deltaFieldsEntrySize, size_t fullRecordEntrySize);

    void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) override;
    void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    /// Attempts to load the predecessor buffer's last delta field values.
    /// Returns true if found (values copied to destPtr), false otherwise.
    bool loadPredecessor(SequenceNumber seqNum, std::byte* destPtr);

    /// Stores this buffer's last delta field values and checks if the successor
    /// buffer has a pending first record. Returns true if pending found (copied to destPtr).
    bool storeLastAndCheckPending(SequenceNumber seqNum, const std::byte* srcPtr, std::byte* destPtr);

    /// Stores the first record of this buffer as pending and checks if the predecessor's
    /// last delta values are already available (handles the case where the predecessor closed
    /// before this buffer processed its first record). Returns true if predecessor found
    /// (delta field values copied to predecessorDest), false otherwise.
    bool storePendingAndCheckPredecessor(SequenceNumber seqNum, const std::byte* fullRecordSrc, std::byte* predecessorDest);

private:
    struct HandlerState
    {
        /// Buffer N stores its last delta-field values here when it finishes (close).
        std::unordered_map<SequenceNumber, std::unique_ptr<std::byte[]>> lastTuples;
        /// Buffer N stores its first record here when predecessor N-1 hasn't finished yet.
        std::unordered_map<SequenceNumber, std::unique_ptr<std::byte[]>> pendingFirstRecords;
    };

    folly::Synchronized<HandlerState> state;
    size_t deltaFieldsEntrySize;
    size_t fullRecordEntrySize;
};

}
