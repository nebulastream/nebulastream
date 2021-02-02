/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Sources/AdaptiveSource.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Util/ThreadNaming.hpp>
#include <cassert>
#include <unistd.h>

namespace NES {

AdaptiveSource::AdaptiveSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager,
                               NodeEngine::QueryManagerPtr queryManager, uint64_t initialGatheringInterval, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId) {
    NES_DEBUG("AdaptiveSource:" << this << " creating with interval:" << initialGatheringInterval);
    this->gatheringInterval = initialGatheringInterval;
}

SourceType AdaptiveSource::getType() const { return ADAPTIVE_SOURCE; }

std::optional<NodeEngine::TupleBuffer> AdaptiveSource::receiveData() {
    NES_DEBUG("AdaptiveSource::receiveData called");
    auto buf = this->bufferManager->getBufferBlocking();
    this->sampleSourceAndFillBuffer(buf);
    this->decideNewGatheringInterval();
    NES_DEBUG("AdaptiveSource::receiveData finished");
    return buf;
}

void AdaptiveSource::runningRoutine(NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager) {
    setThreadName("AdaptSrc-%d", operatorId);
    std::string thName = "AdaptSrc-" + operatorId;

    if (!bufferManager) {
        NES_ERROR("AdaptiveSource:" << this << ", BufferManager not set");
        throw std::logic_error("AdaptiveSource: BufferManager not set");
    }

    if (!queryManager) {
        NES_ERROR("AdaptiveSource:" << this << ", QueryManager not set");
        throw std::logic_error("AdaptiveSource: QueryManager not set");
    }

    if (this->operatorId == 0) {
        NES_FATAL_ERROR("AdaptiveSource: No ID assigned. Running_routine is not possible!");
        throw std::logic_error("AdaptiveSource: No ID assigned. Running_routine is not possible!");
    }

    NES_DEBUG("AdaptiveSource " << this->operatorId << ": Running Data Source of type=" << this->getType());
    uint64_t cnt = 0;

    while (this->isRunning()) {
        uint64_t currentTime = time(NULL);
        if (gatheringInterval == 0 || (lastGatheringTimeStamp != currentTime && currentTime % this->gatheringInterval == 0)) {
            lastGatheringTimeStamp = currentTime;
            if (cnt < numBuffersToProcess) {
                auto optBuf = this->receiveData();
                if (optBuf.has_value()) {
                    auto& buf = optBuf.value();
                    NES_DEBUG("AdaptiveSource " << this->operatorId << " string=" << this->toString()
                                                << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                                << " iteration=" << cnt);

                    queryManager->addWork(this->operatorId, buf);
                    cnt++;
                }
            }
            NES_DEBUG("AdaptiveSource::runningRoutine running " << this);
        } else {
            NES_DEBUG("AdaptiveSource::runningRoutine sleep " << this);
            sleep(this->gatheringInterval);
            continue;
        }
    }
    NES_DEBUG("AdaptiveSource " << this->operatorId << ": end running");
}
}// namespace NES