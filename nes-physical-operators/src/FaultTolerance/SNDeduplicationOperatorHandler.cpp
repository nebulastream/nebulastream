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

#include <FaultTolerance/SNDeduplicationOperatorHandler.hpp>

#include <Identifiers/Identifiers.hpp>
#include <absl/strings/internal/str_format/extension.h>

#include <nameof.hpp>

namespace NES
{

SNDeduplicationOperatorHandler::SNDeduplicationOperatorHandler(std::string filePath, const std::vector<OriginId>& inputOrigins)
    : filePath{filePath}, origins{inputOrigins}
{
    for (auto originId : origins)
    {
        gcMutexes.emplace(originId, std::make_unique<std::mutex>());
    }
}

void SNDeduplicationOperatorHandler::init()
{
    // TODO
    /*seenSequenceNumbers.clear();
    for (auto originId : origins)
    {
        seenSequenceNumbers.emplace(originId, std::make_shared<folly::Synchronized<std::unordered_set<SequenceNumber>>>());
    }
    if (std::filesystem::exists(filePath))
    {
        std::ifstream in(filePath);
        if (!in)
            throw std::runtime_error("uhm? file error");

        u_int64_t originId;
        u_int64_t sequenceNumber;
        while (in >> originId >> sequenceNumber)
        {
            auto synSet = seenSequenceNumbers[OriginId{originId}];
            auto snSet = synSet->wlock();
            snSet->emplace(SequenceNumber{sequenceNumber});
        }
    } */
}

void SNDeduplicationOperatorHandler::save()
{
    // TODO
    /*std::ofstream out(filePath);
    if (!out)
        throw std::runtime_error("uhm? file error");
    for (auto [originId, synSet] : seenSequenceNumbers)
    {
        auto snSet = synSet->wlock();
        for (auto sn : *snSet)
        {
            out << originId << ' ' << sn << '\n';
        }
    } */
}

bool SNDeduplicationOperatorHandler::filterAndBuffer(TupleBuffer* buffer)
{
    auto& syncSnMap = buffers[buffer->getOriginId()];
    auto snMap = syncSnMap.wlock();

    if (snMap->lowWatermark >= buffer->getSequenceNumber())
    {
        return false;
    }

    auto [it, inserted] = snMap->records.try_emplace(buffer->getSequenceNumber(), folly::Synchronized<detail::SNBufferingRecord>{});
    auto* syncBufferingRecord = &it->second;

    auto bufferingRecord = syncBufferingRecord->wlock();
    snMap.unlock();

    /// completed SN that was not yet GCed; ignore
    if (bufferingRecord->completed)
    {
        return false;
    }

    /// detected new epoch for an uncompleted SN; restart SN
    if (bufferingRecord->originEpoch != buffer->getOriginEpoch())
    {
        bufferingRecord->buffers.clear();
        bufferingRecord->maxChunkNumber = ChunkNumber{ChunkNumber::INVALID};
        bufferingRecord->originEpoch = buffer->getOriginEpoch();
    }

    /// now we know the amount of chunks for the SN; may not be the final chunk due to out-of-order processing
    if (buffer->isLastChunk())
    {
        bufferingRecord->maxChunkNumber = buffer->getChunkNumber();
    }

    bufferingRecord->buffers.push_back(*buffer);

    /// complete the SN if the expected number of chunks has been seen for the current epoch
    if (bufferingRecord->buffers.size() == bufferingRecord->maxChunkNumber.getRawValue())
    {
        bufferingRecord->completed = true;
        return true;
    }
    return false;
}

TupleBuffer* SNDeduplicationOperatorHandler::probe(TupleBuffer* buffer)
{
    auto& syncSnMap = buffers[buffer->getOriginId()];
    auto snMap = syncSnMap.wlock();
    auto it = snMap->records.find(buffer->getSequenceNumber());
    if (it == snMap->records.end())
    {
        return nullptr;
    }
    auto* syncBufferingRecord = &it->second;

    auto bufferingRecord = syncBufferingRecord->wlock();
    snMap.unlock();

    if (bufferingRecord->iterator == bufferingRecord->buffers.size())
    {
        return nullptr;
    }
    auto& ret = bufferingRecord->buffers.at(bufferingRecord->iterator);
    bufferingRecord->iterator += 1;

    return &ret;
}

void SNDeduplicationOperatorHandler::finalizeSN(TupleBuffer* buffer)
{
    auto& syncSnMap = buffers[buffer->getOriginId()];
    auto snMap = syncSnMap.wlock();
    auto* syncBufferingRecord = &snMap->records.find(buffer->getSequenceNumber())->second;
    auto bufferingRecord = syncBufferingRecord->wlock();
    snMap.unlock();

    bufferingRecord->finalized = true;
    bufferingRecord->buffers.clear();
    bufferingRecord->buffers.shrink_to_fit();
    bufferingRecord.unlock();

    // try to enter GC routine, otherwise skip. Does not have to be completely up to date
    auto& mutex = gcMutexes.at(buffer->getOriginId());
    std::unique_lock<std::mutex> lock(*mutex, std::try_to_lock);
    if (lock.owns_lock())
    {
        snMap = syncSnMap.wlock();
        while (true)
        {
            auto nextLWM = SequenceNumber(snMap->lowWatermark.getRawValue() + 1);

            auto itNext = snMap->records.find(nextLWM);
            if (itNext == snMap->records.end())
            {
                break;
            }

            // ensure that no one is still holding a lock on the record
            // this could happen if another thread has released the map lock and is still working on the record
            auto rec = itNext->second.tryWLock();
            if (!rec)
            {
                break;
            }

            if (!rec->finalized)
            {
                break;
            }
            // it's safe to unlock the record as locks on records are only acquired while holding a lock on the parent map
            rec.unlock();

            snMap->records.erase(itNext);
            snMap->lowWatermark = nextLWM;
        }
    }
}
}
