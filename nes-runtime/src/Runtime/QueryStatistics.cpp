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

#include <Identifiers/Identifiers.hpp>
#include <Runtime/QueryStatistics.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>

namespace NES::Runtime {

uint64_t QueryStatistics::getProcessedTasks() const { return processedTasks.load(); }

uint64_t QueryStatistics::getProcessedTuple() const { return processedTuple.load(); }

uint64_t QueryStatistics::getProcessedBuffers() const { return processedBuffers.load(); }

uint64_t QueryStatistics::getTimestampQueryStart() const { return timestampQueryStart.load(); }

uint64_t QueryStatistics::getTimestampFirstProcessedTask() const { return timestampFirstProcessedTask.load(); }

uint64_t QueryStatistics::getTimestampLastProcessedTask() const { return timestampLastProcessedTask.load(); }

uint64_t QueryStatistics::getProcessedWatermarks() const { return processedWatermarks.load(); }

uint64_t QueryStatistics::getLatencySum() const { return latencySum.load(); }

uint64_t QueryStatistics::getQueueSizeSum() const { return queueSizeSum.load(); }

uint64_t QueryStatistics::getAvailableGlobalBufferSum() const { return availableGlobalBufferSum.load(); }
uint64_t QueryStatistics::getAvailableFixedBufferSum() const { return availableFixedBufferSum.load(); }

void QueryStatistics::setProcessedTasks(uint64_t processedTasks) { this->processedTasks = processedTasks; }

void QueryStatistics::setProcessedTuple(uint64_t processedTuple) { this->processedTuple = processedTuple; }

void QueryStatistics::setTimestampQueryStart(uint64_t timestampQueryStart, bool noOverwrite = false) {
    if (!noOverwrite || this->timestampQueryStart == 0) {
        NES_DEBUG("QueryStatistics::setTimestampQueryStart called with  {}", timestampQueryStart);
        this->timestampQueryStart = timestampQueryStart;
    }
}
void QueryStatistics::setTimestampFirstProcessedTask(uint64_t timestampFirstProcessedTask, bool noOverwrite = false) {
    if (!noOverwrite || this->timestampFirstProcessedTask == 0) {
        NES_DEBUG("QueryStatistics::setTimestampFirstProcessedTask called with  {}", timestampFirstProcessedTask);
        this->timestampFirstProcessedTask = timestampFirstProcessedTask;
    }
}

void QueryStatistics::setTimestampLastProcessedTask(uint64_t timestampLastProcessedTask) {
    this->timestampLastProcessedTask = timestampLastProcessedTask;
}

void QueryStatistics::incProcessedBuffers() { this->processedBuffers++; }

void QueryStatistics::incProcessedTasks() { this->processedTasks++; }

void QueryStatistics::incProcessedWatermarks() { this->processedWatermarks++; }
void QueryStatistics::incProcessedTuple(uint64_t tupleCnt) { this->processedTuple += tupleCnt; }
void QueryStatistics::incLatencySum(uint64_t latency) { this->latencySum += latency; }
void QueryStatistics::incTasksPerPipelineId(PipelineId pipelineId, WorkerThreadId workerId) {
    (*this->pipelineIdToTaskThroughputMap.wlock())[pipelineId][workerId]++;
}
void QueryStatistics::incQueueSizeSum(uint64_t size) { this->queueSizeSum += size; }
void QueryStatistics::incAvailableGlobalBufferSum(uint64_t size) { this->availableGlobalBufferSum += size; }
void QueryStatistics::incAvailableFixedBufferSum(uint64_t size) { this->availableFixedBufferSum += size; }

void QueryStatistics::setProcessedBuffers(uint64_t processedBuffers) { this->processedBuffers = processedBuffers; }

void QueryStatistics::addTimestampToLatencyValue(uint64_t now, uint64_t latency) {
    (*tsToLatencyMap.wlock())[now].push_back(latency);
}

folly::Synchronized<std::map<PipelineId, std::map<WorkerThreadId, std::atomic<uint64_t>>>>&
QueryStatistics::getPipelineIdToTaskMap() {
    return pipelineIdToTaskThroughputMap;
};

folly::Synchronized<std::map<uint64_t, std::vector<uint64_t>>>& QueryStatistics::getTsToLatencyMap() { return tsToLatencyMap; }

std::string QueryStatistics::getQueryStatisticsAsString() {
    std::stringstream ss;
    ss << queryId.load() << ",";
    ss << subQueryId.load() << ",";
    ss << processedTasks.load() << ",";
    ss << processedTuple.load() << ",";
    ss << processedBuffers.load() << ",";
    ss << processedWatermarks.load() << ",";
    ss << latencySum.load() / (processedBuffers.load() == 0 ? 1 : processedBuffers.load()) << ",";
    ss << queueSizeSum.load() / (processedBuffers.load() == 0 ? 1 : processedBuffers.load()) << ",";
    ss << availableGlobalBufferSum.load() / (processedBuffers.load() == 0 ? 1 : processedBuffers.load()) << ",";
    ss << availableFixedBufferSum.load() / (processedBuffers.load() == 0 ? 1 : processedBuffers.load()) << ",";
    return ss.str();
}

void QueryStatistics::clear() {
    processedTasks = 0;
    processedTuple = 0;
    processedBuffers = 0;
    processedWatermarks = 0;
    latencySum = 0;
    queueSizeSum = 0;
    availableGlobalBufferSum = 0;
    availableFixedBufferSum = 0;
}

SharedQueryId QueryStatistics::getQueryId() const { return queryId.load(); }
DecomposedQueryId QueryStatistics::getSubQueryId() const { return subQueryId.load(); }

QueryStatistics::QueryStatistics(const QueryStatistics& other) {
    processedTasks = other.processedTasks.load();
    processedTuple = other.processedTuple.load();
    processedBuffers = other.processedBuffers.load();
    processedWatermarks = other.processedWatermarks.load();
    latencySum = other.latencySum.load();
    queueSizeSum = other.queueSizeSum.load();
    availableGlobalBufferSum = other.availableGlobalBufferSum.load();
    availableFixedBufferSum = other.availableFixedBufferSum.load();
    queryId = other.queryId.load();
    subQueryId = other.subQueryId.load();
    tsToLatencyMap = other.tsToLatencyMap;
}

}// namespace NES::Runtime
