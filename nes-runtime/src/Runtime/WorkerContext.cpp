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

#include <Network/NetworkChannel.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <sys/time.h>

namespace NES::Runtime {

folly::ThreadLocalPtr<LocalBufferPool> WorkerContext::localBufferPoolTLS{};

WorkerContext::WorkerContext(uint32_t workerId,
                             const BufferManagerPtr& bufferManager,
                             uint64_t numberOfBuffersPerWorker,
                             uint32_t queueId)
    : workerId(workerId), queueId(queueId) {
    //we changed from a local pool to a fixed sized pool as it allows us to manage the numbers that are hold in the cache via the paramter
    localBufferPool = bufferManager->createLocalBufferPool(numberOfBuffersPerWorker);
    localBufferPoolTLS.reset(localBufferPool.get(), [](auto*, folly::TLPDestructionMode) {
        // nop
    });
    NES_ASSERT(!!localBufferPool, "Local buffer is not allowed to be null");
    NES_ASSERT(!!localBufferPoolTLS, "Local buffer is not allowed to be null");
    statisticsFile.open("latency" + std::to_string(workerId) + ".csv", std::ios::out);
//    propagationFile.open("propagation" + std::to_string(workerId) + ".csv", std::ios::out);
//    storageFile.open("storage" + std::to_string(workerId) + ".csv", std::ios::out);
    statisticsFile << "time,latency\n";
//    propagationFile << "time,propagationDelay,difference\n";
//    storageFile << "time,oldStorageSize,newStorageSize\n";
}

WorkerContext::~WorkerContext() {
    localBufferPool->destroy();
    localBufferPoolTLS.reset(nullptr);
//    storageFile.flush();
//    storageFile.close();
//    propagationFile.flush();
//    propagationFile.close();
    statisticsFile.flush();
    statisticsFile.close();
}

size_t WorkerContext::getStorageSize() {
    auto size = 0;
    for (auto iteratorPartitionId : this->storage) {
        size += iteratorPartitionId.second.size();
    }
    return size;
}

void WorkerContext::printStatistics(Runtime::TupleBuffer& inputBuffer) {
    auto now = std::chrono::system_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
    auto epoch = now_ms.time_since_epoch();
    auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
    auto ts = std::chrono::system_clock::now();
    timeval curTime;
    gettimeofday(&curTime, NULL);
    int milli = curTime.tv_usec / 1000;
    auto timeNow = std::chrono::system_clock::to_time_t(ts);
    char buffer[26];
    int millisec;
    struct tm* tm_info;
    struct timeval tv;

    gettimeofday(&tv, NULL);

    millisec = lrint(tv.tv_usec / 1000.0);// Round to nearest millisec
    if (millisec >= 1000) {               // Allow for rounding up to nearest second
        millisec -= 1000;
        tv.tv_sec++;
    }

    tm_info = localtime(&tv.tv_sec);

    strftime(buffer, 26, "%Y:%m:%d %H:%M:%S", tm_info);
    statisticsFile << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %H:%M:%S") << ".";
    if (millisec < 10)
        statisticsFile << "00" << millisec << ",";
    else if (millisec < 100)
        statisticsFile << "0" << millisec << ",";
    else
        statisticsFile << millisec << ",";
    statisticsFile << value.count() - inputBuffer.getWatermark() << "\n";
    statisticsFile.flush();
}

uint32_t WorkerContext::getId() const { return workerId; }

uint32_t WorkerContext::getQueueId() const { return queueId; }

void WorkerContext::setObjectRefCnt(void* object, uint32_t refCnt) {
    objectRefCounters[reinterpret_cast<uintptr_t>(object)] = refCnt;
}

uint32_t WorkerContext::increaseObjectRefCnt(void* object) { return objectRefCounters[reinterpret_cast<uintptr_t>(object)]++; }

uint32_t WorkerContext::decreaseObjectRefCnt(void* object) {
    auto ptr = reinterpret_cast<uintptr_t>(object);
    if (auto it = objectRefCounters.find(ptr); it != objectRefCounters.end()) {
        auto val = it->second--;
        if (val == 1) {
            objectRefCounters.erase(it);
        }
        return val;
    }
    return 0;
}

TupleBuffer WorkerContext::allocateTupleBuffer() { return localBufferPool->getBufferBlocking(); }

void WorkerContext::storeNetworkChannel(NES::OperatorId id, Network::NetworkChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator " << id << " for context " << workerId);
    dataChannels[id] = std::move(channel);
}

void WorkerContext::createStorage(Network::NesPartition nesPartitionId) {
    this->storage[nesPartitionId] = std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, BufferOrdering>();
}

void WorkerContext::insertIntoStorage(Network::NesPartition nesPartitionId, NES::Runtime::TupleBuffer buffer) {
    storage[nesPartitionId].push(buffer);
}

std::priority_queue<TupleBuffer, std::vector<TupleBuffer>, BufferOrdering> WorkerContext::resendDataFromDataStorage(Network::NesPartition nesPartitionId){
    return this->storage[nesPartitionId];
}

void WorkerContext::trimStorage(Network::NesPartition nesPartitionId, uint64_t timestamp, uint64_t) {
    NES_DEBUG("BufferStorage: Received trimming message with a timestamp " << timestamp);
    auto iteratorPartitionId = this->storage.find(nesPartitionId);
    if (iteratorPartitionId != this->storage.end()) {
        auto& [nesPar, pq] = *iteratorPartitionId;
//        auto oldStorageSize = pq.size();
        while (!pq.empty()) {
            auto topWatermark = pq.top().getWatermark();
            if (topWatermark <= timestamp) {
                pq.pop();
            } else {
                break;
            }
        }
//        auto now = std::chrono::system_clock::now();
//        auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
//        auto epoch = now_ms.time_since_epoch();
//        auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
//        auto ts = std::chrono::system_clock::now();
//        auto timeNow = std::chrono::system_clock::to_time_t(ts);
//        propagationFile << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %X") << ",";
//        propagationFile << propagationDelay << ",";
//        propagationFile << value.count() - propagationDelay << "\n";
//        timeval curTime;
//        gettimeofday(&curTime, NULL);
//        int milli = curTime.tv_usec / 1000;
//        char buffer[26];
//        int millisec;
//        struct tm* tm_info;
//        struct timeval tv;
//
//        gettimeofday(&tv, NULL);
//
//        millisec = lrint(tv.tv_usec / 1000.0);// Round to nearest millisec
//        if (millisec >= 1000) {               // Allow for rounding up to nearest second
//            millisec -= 1000;
//            tv.tv_sec++;
//        }
//
//        tm_info = localtime(&tv.tv_sec);
//
//        strftime(buffer, 26, "%Y:%m:%d %H:%M:%S", tm_info);
//        storageFile << std::put_time(std::localtime(&timeNow), "%Y-%m-%d %H:%M:%S") << ".";
//        if (millisec < 10)
//            storageFile << "00" << millisec << ",";
//        else if (millisec < 100)
//            storageFile << "0" << millisec << ",";
//        else
//            storageFile << millisec << ",";
//        NES_DEBUG("BufferStorage: Deleted old size " << oldStorageSize << " tuples");
//        NES_DEBUG("BufferStorage: Deleted new size " << pq.size() << " tuples");
//        NES_DEBUG("BufferStorage: Deleted diff " << oldStorageSize - pq.size() << " tuples");
//        storageFile << oldStorageSize << ",";
//        storageFile << pq.size() << "\n";
//        storageFile.flush();
    }
}

bool WorkerContext::releaseNetworkChannel(NES::OperatorId id, Runtime::QueryTerminationType terminationType) {
    NES_TRACE("WorkerContext: releasing channel for operator " << id << " for context " << workerId);
    if (auto it = dataChannels.find(id); it != dataChannels.end()) {
        if (auto& channel = it->second; channel) {
            channel->close(terminationType);
        }
        dataChannels.erase(it);
        return true;
    }
    return false;
}

void WorkerContext::storeEventOnlyChannel(NES::OperatorId id, Network::EventOnlyNetworkChannelPtr&& channel) {
    NES_TRACE("WorkerContext: storing channel for operator " << id << " for context " << workerId);
    reverseEventChannels[id] = std::move(channel);
}

bool WorkerContext::releaseEventOnlyChannel(NES::OperatorId id, Runtime::QueryTerminationType terminationType) {
    NES_TRACE("WorkerContext: releasing channel for operator " << id << " for context " << workerId);
    if (auto it = reverseEventChannels.find(id); it != reverseEventChannels.end()) {
        if (auto& channel = it->second; channel) {
            channel->close(terminationType);
        }
        reverseEventChannels.erase(it);
        return true;
    }
    return false;
}

Network::NetworkChannel* WorkerContext::getNetworkChannel(NES::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving channel for operator " << ownerId << " for context " << workerId);
    auto it = dataChannels.find(ownerId);// note we assume it's always available
    return (*it).second.get();
}

Network::EventOnlyNetworkChannel* WorkerContext::getEventOnlyNetworkChannel(NES::OperatorId ownerId) {
    NES_TRACE("WorkerContext: retrieving event only channel for operator " << ownerId << " for context " << workerId);
    auto it = reverseEventChannels.find(ownerId);// note we assume it's always available
    return (*it).second.get();
}

LocalBufferPool* WorkerContext::getBufferProviderTLS() { return localBufferPoolTLS.get(); }

LocalBufferPoolPtr WorkerContext::getBufferProvider() { return localBufferPool; }

}// namespace NES::Runtime