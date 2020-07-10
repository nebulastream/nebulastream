#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <SourceSink/AdaptiveSource.hpp>
#include <Util/UtilityFunctions.hpp>

#include <cassert>
#include <unistd.h>

namespace NES {

AdaptiveSource::AdaptiveSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, size_t initialGatheringInterval)
    : DataSource(schema, bufferManager, queryManager) {
    NES_DEBUG("AdaptiveSource:" << this << " creating with interval:" << initialGatheringInterval);
    this->gatheringInterval = initialGatheringInterval;
}

SourceType AdaptiveSource::getType() const {
    return ADAPTIVE_SOURCE;
}

std::optional<TupleBuffer> AdaptiveSource::receiveData() {
    NES_DEBUG("AdaptiveSource::receiveData called");
    auto buf = this->bufferManager->getBufferBlocking();
    this->sampleSourceAndFillBuffer(buf);
    this->decideNewGatheringInterval();
    NES_DEBUG("AdaptiveSource::receiveData finished");
    return buf;
}

void AdaptiveSource::runningRoutine(BufferManagerPtr bufferManager, QueryManagerPtr queryManager) {
    if (!bufferManager) {
        NES_ERROR("AdaptiveSource:" << this << ", BufferManager not set");
        throw std::logic_error("AdaptiveSource: BufferManager not set");
    }

    if (!queryManager) {
        NES_ERROR("AdaptiveSource:" << this << ", QueryManager not set");
        throw std::logic_error("AdaptiveSource: QueryManager not set");
    }

    if (this->sourceId.empty()) {
        NES_FATAL_ERROR("AdaptiveSource: No ID assigned. Running_routine is not possible!");
        throw std::logic_error("AdaptiveSource: No ID assigned. Running_routine is not possible!");
    }

    NES_DEBUG("AdaptiveSource " << this->getSourceId() << ": Running Data Source of type=" << this->getType());
    size_t cnt = 0;

    while (this->isRunning()) {
        size_t currentTime = time(NULL);
        if (gatheringInterval == 0 || (lastGatheringTimeStamp != currentTime && currentTime % this->gatheringInterval == 0)) {
            lastGatheringTimeStamp = currentTime;
            if (cnt < numBuffersToProcess) {
                auto optBuf = this->receiveData();
                if (!!optBuf) {
                    auto& buf = optBuf.value();
                    NES_DEBUG("AdaptiveSource " << this->getSourceId()
                                                << " string=" << this->toString()
                                                << ": Received Data: " << buf.getNumberOfTuples() << " tuples"
                                                << " iteration=" << cnt);

                    queryManager->addWork(this->sourceId, buf);
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
    NES_DEBUG("AdaptiveSource " << this->getSourceId() << ": end running");
}
}// namespace NES