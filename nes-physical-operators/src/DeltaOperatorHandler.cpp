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

#include <ErrorHandling.hpp>

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

bool DeltaOperatorHandler::loadPredecessor(SequenceNumber, std::byte*)
{
    /// TODO step-5: look up the predecessor buffer's stored last delta-field values.
    ///   * Compute predecessorSeq = seqNum - 1.
    ///   * Under the write lock, find lastTuples[predecessorSeq].
    ///   * If found: memcpy into destPtr, erase the entry, return true.
    ///   * Otherwise: return false.
    throw NotImplemented("TODO step-5: implement DeltaOperatorHandler::loadPredecessor");
}

bool DeltaOperatorHandler::storeLastAndCheckPending(SequenceNumber, const std::byte*, std::byte*)
{
    /// TODO step-5: store this buffer's last delta-field values and resolve any pending successor.
    ///   * Under the write lock, store srcPtr (deltaFieldsEntrySize bytes) at lastTuples[seqNum].
    ///   * Look up pendingFirstRecords[seqNum + 1]; if present, memcpy into destPtr, erase, return true.
    ///   * Otherwise: return false.
    throw NotImplemented("TODO step-5: implement DeltaOperatorHandler::storeLastAndCheckPending");
}

bool DeltaOperatorHandler::storePendingAndCheckPredecessor(SequenceNumber, const std::byte*, std::byte*)
{
    /// TODO step-5: try the predecessor first, otherwise park as pending.
    ///   * Under the write lock, look up lastTuples[seqNum - 1].
    ///   * If found: memcpy delta values into predecessorDest, erase the entry, return true.
    ///   * Otherwise: store fullRecordSrc (fullRecordEntrySize bytes) at pendingFirstRecords[seqNum], return false.
    throw NotImplemented("TODO step-5: implement DeltaOperatorHandler::storePendingAndCheckPredecessor");
}

}
