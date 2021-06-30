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

#include <Runtime/BufferManager.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/AdaptiveSource.hpp>
#include <Util/ThreadNaming.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cassert>
#include <chrono>
#include <limits>
#include <thread>
#include <unistd.h>
#include <utility>

namespace NES {

AdaptiveSource::AdaptiveSource(SchemaPtr schema,
                               Runtime::BufferManagerPtr bufferManager,
                               Runtime::QueryManagerPtr queryManager,
                               uint64_t initialGatheringInterval,
                               OperatorId operatorId,
                               OperatorId logicalSourceOperatorId,
                               size_t numSourceLocalBuffers,
                               GatheringMode gatheringMode)
    : DataSource(std::move(schema),
                 std::move(bufferManager),
                 std::move(queryManager),
                 operatorId,
                 logicalSourceOperatorId,
                 numSourceLocalBuffers,
                 gatheringMode,
                 std::vector<Runtime::Execution::SuccessorExecutablePipeline>()) {
    NES_DEBUG("AdaptiveSource:" << this << " creating with interval:" << initialGatheringInterval << "ms");
    this->gatheringInterval = std::chrono::milliseconds(initialGatheringInterval);
}

SourceType AdaptiveSource::getType() const { return ADAPTIVE_SOURCE; }

std::optional<Runtime::TupleBuffer> AdaptiveSource::receiveData() {
    NES_DEBUG("AdaptiveSource::receiveData called");
    auto buf = this->bufferManager->getBufferBlocking();
    this->sampleSourceAndFillBuffer(buf);
    this->decideNewGatheringInterval();
    NES_DEBUG("AdaptiveSource::receiveData finished");
    return buf;
}

void AdaptiveSource::runningRoutine() {
    setThreadName("AdaptSrc-%d", operatorId);

    if (this->operatorId == 0) {
        NES_FATAL_ERROR("AdaptiveSource: No ID assigned. Running_routine is not possible!");
        throw std::logic_error("AdaptiveSource: No ID assigned. Running_routine is not possible!");
    }

    NES_DEBUG("AdaptiveSource " << this->operatorId << ": Running Data Source of type=" << this->getType());
    uint64_t cnt = 0;

    auto zeroSecInMillis = std::chrono::milliseconds(0);
    open();
    while (this->isRunning()) {
        auto tsNow = std::chrono::system_clock::now();
        std::chrono::milliseconds nowInMillis = std::chrono::duration_cast<std::chrono::milliseconds>(tsNow.time_since_epoch());

        if (gatheringInterval == zeroSecInMillis
            || (lastGatheringTimeStamp != nowInMillis
                && (nowInMillis - lastGatheringTimeStamp <= this->gatheringInterval
                    || (nowInMillis - lastGatheringTimeStamp % this->gatheringInterval).count() == 0))) {
            lastGatheringTimeStamp = nowInMillis;
            if (cnt < numBuffersToProcess) {
                auto optBuf = this->receiveData();
                if (optBuf.has_value()) {
                    auto& buf = optBuf.value();
                    NES_DEBUG("AdaptiveSource " << this->operatorId << " string=" << this->toString()
                                                << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                                << " iteration=" << cnt);

                    emitWorkFromSource(buf);
                    cnt++;
                }
            }
            NES_DEBUG("AdaptiveSource::runningRoutine running " << this);
        } else {
            NES_DEBUG("AdaptiveSource::runningRoutine sleep " << this);
            std::this_thread::sleep_for(this->gatheringInterval);
            continue;
        }
    }
    queryManager->addEndOfStream(shared_from_base<DataSource>(), true);
    NES_DEBUG("AdaptiveSource " << this->operatorId << ": end running");
}
}// namespace NES