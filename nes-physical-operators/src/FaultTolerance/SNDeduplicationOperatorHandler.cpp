
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
}

void SNDeduplicationOperatorHandler::init()
{
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
    folly::Synchronized<detail::SNBufferingRecord>* syncBufferingRecord;
    if (!snMap->contains(buffer->getSequenceNumber()))
    {
        snMap->insert({buffer->getSequenceNumber(), folly::Synchronized<detail::SNBufferingRecord>{}});
    }
    syncBufferingRecord = &snMap->find(buffer->getSequenceNumber())->second;
    snMap.unlock();

    auto bufferingRecord = syncBufferingRecord->wlock();
    if (bufferingRecord->completed)
    {
        return false;
    }
    if (bufferingRecord->originEpoch != buffer->getOriginEpoch())
    {
        bufferingRecord->buffers.clear();
        bufferingRecord->maxChunkNumber = ChunkNumber{ChunkNumber::INVALID};
        bufferingRecord->originEpoch = buffer->getOriginEpoch();
    }

    if (buffer->isLastChunk())
    {
        bufferingRecord->maxChunkNumber = buffer->getChunkNumber();
    }

    bufferingRecord->buffers.push_back(*buffer);

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
    if (!snMap->contains(buffer->getSequenceNumber()))
    {
        return nullptr;
    }
    auto syncBufferingRecord = &snMap->find(buffer->getSequenceNumber())->second;
    snMap.unlock();

    auto bufferingRecord = syncBufferingRecord->wlock();
    if (bufferingRecord->iterator == bufferingRecord->buffers.size())
    {
        return nullptr;
    }
    auto ret = &bufferingRecord->buffers[bufferingRecord->iterator];
    bufferingRecord->iterator += 1;

    return ret;
}

//TODO add some gargbage collection to summarize history (e.g. store highest SN until which there are no gaps)
void SNDeduplicationOperatorHandler::finalizeSN(TupleBuffer* buffer)
{
    auto& syncSnMap = buffers[buffer->getOriginId()];
    auto snMap = syncSnMap.wlock();
    auto syncBufferingRecord = &snMap->find(buffer->getSequenceNumber())->second;
    snMap.unlock();
    auto bufferingRecord = syncBufferingRecord->wlock();
    bufferingRecord->buffers.clear();
}
}
