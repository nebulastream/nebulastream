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

#include <DeltaOperatorHandler.hpp>

#include <cstring>

namespace NES
{

DeltaOperatorHandler::DeltaOperatorHandler(size_t deltaFieldsEntrySize, size_t fullRecordEntrySize)
    : deltaFieldsEntrySize(deltaFieldsEntrySize), fullRecordEntrySize(fullRecordEntrySize)
{
}

void DeltaOperatorHandler::start(PipelineExecutionContext&, uint32_t)
{
}

void DeltaOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

bool DeltaOperatorHandler::loadPredecessor(SequenceNumber seqNum, std::byte* destPtr)
{
    const auto predecessorSeq = SequenceNumber(seqNum.getRawValue() - 1);
    const auto lock = state.wlock();
    auto it = lock->lastTuples.find(predecessorSeq);
    if (it != lock->lastTuples.end())
    {
        std::memcpy(destPtr, it->second.get(), deltaFieldsEntrySize);
        lock->lastTuples.erase(it);
        return true;
    }
    return false;
}

bool DeltaOperatorHandler::storeLastAndCheckPending(SequenceNumber seqNum, const std::byte* srcPtr, std::byte* destPtr)
{
    const auto successorSeq = SequenceNumber(seqNum.getRawValue() + 1);
    const auto lock = state.wlock();

    /// Store this buffer's last delta field values.
    auto lastEntry = std::make_unique<std::byte[]>(deltaFieldsEntrySize);
    std::memcpy(lastEntry.get(), srcPtr, deltaFieldsEntrySize);
    lock->lastTuples[seqNum] = std::move(lastEntry);

    /// Check if the successor buffer has stored a pending first record.
    bool found = false;
    auto it = lock->pendingFirstRecords.find(successorSeq);
    if (it != lock->pendingFirstRecords.end())
    {
        std::memcpy(destPtr, it->second.get(), fullRecordEntrySize);
        lock->pendingFirstRecords.erase(it);
        found = true;
    }

    return found;
}

bool DeltaOperatorHandler::storePendingAndCheckPredecessor(
    SequenceNumber seqNum, const std::byte* fullRecordSrc, std::byte* predecessorDest)
{
    const auto predecessorSeq = SequenceNumber(seqNum.getRawValue() - 1);
    const auto lock = state.wlock();

    /// Check if the predecessor's last values are already available
    /// (predecessor closed before we processed our first record).
    auto predIt = lock->lastTuples.find(predecessorSeq);
    if (predIt != lock->lastTuples.end())
    {
        std::memcpy(predecessorDest, predIt->second.get(), deltaFieldsEntrySize);
        lock->lastTuples.erase(predIt);
        /// Don't store pending — we resolve immediately in traced code.
        return true;
    }

    /// Predecessor not available yet. Store the full first record as pending
    /// so the predecessor can resolve it in its close().
    auto entry = std::make_unique<std::byte[]>(fullRecordEntrySize);
    std::memcpy(entry.get(), fullRecordSrc, fullRecordEntrySize);
    lock->pendingFirstRecords[seqNum] = std::move(entry);
    return false;
}

}
