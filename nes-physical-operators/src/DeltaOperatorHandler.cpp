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

bool DeltaOperatorHandler::loadPredecessor(SequenceNumber /* seqNum */, std::byte* /* destPtr */)
{
    /// TODO: implement loadPredecessor(). NO_TODO_CHECK
    /// Look up the predecessor's (seqNum - 1) last delta field values in lastTuples.
    /// If found, copy them to destPtr and erase the entry. Return true if found.
    return false;
}

bool DeltaOperatorHandler::storeLastAndCheckPending(SequenceNumber /* seqNum */, const std::byte* /* srcPtr */, std::byte* /* destPtr */)
{
    /// TODO: implement storeLastAndCheckPending(). NO_TODO_CHECK
    /// 1. Store this buffer's last delta field values (srcPtr) in lastTuples[seqNum].
    /// 2. Check if the successor (seqNum + 1) has a pending first record in pendingFirstRecords.
    ///    If found, copy to destPtr and erase. Return true if found.
    return false;
}

bool DeltaOperatorHandler::storePendingAndCheckPredecessor(
    SequenceNumber /* seqNum */, const std::byte* /* fullRecordSrc */, std::byte* /* predecessorDest */)
{
    /// TODO: implement storePendingAndCheckPredecessor(). NO_TODO_CHECK
    /// 1. Check if the predecessor's (seqNum - 1) last values are in lastTuples.
    ///    If found, copy to predecessorDest, erase, and return true immediately.
    /// 2. Otherwise, store the full first record (fullRecordSrc) in pendingFirstRecords[seqNum]
    ///    so the predecessor can resolve it in its close(). Return false.
    return false;
}

}
