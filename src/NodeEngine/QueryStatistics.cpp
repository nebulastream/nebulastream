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

#include <NodeEngine/QueryStatistics.hpp>
#include <sstream>
namespace NES::NodeEngine {

const std::atomic<uint64_t> QueryStatistics::getProcessedTasks() const { return processedTasks.load(); }

const std::atomic<uint64_t> QueryStatistics::getProcessedTuple() const { return processedTuple.load(); }

const std::atomic<uint64_t> QueryStatistics::getProcessedBuffers() const { return processedBuffers.load(); }

const std::atomic<uint64_t> QueryStatistics::getProcessedWatermarks() const { return processedWatermarks.load(); }

const std::atomic<uint64_t> QueryStatistics::getReceivedBuffers() const { return receivedBuffers.load(); }

const std::atomic<uint64_t> QueryStatistics::getSentBuffers() const { return sentBuffers.load(); }

const std::atomic<uint64_t> QueryStatistics::getReceivedBytes() const { return receivedBytes.load(); }

const std::atomic<uint64_t> QueryStatistics::getSentBytes() const { return sentBytes.load(); }

void QueryStatistics::setProcessedTasks(const std::atomic<uint64_t>& processedTasks) {
    this->processedTasks = processedTasks.load();
}

void QueryStatistics::setProcessedTuple(const std::atomic<uint64_t>& processedTuple) {
    this->processedTuple = processedTuple.load();
}

void QueryStatistics::incProcessedBuffers() { this->processedBuffers++; }

void QueryStatistics::incProcessedTasks() { this->processedTasks++; }

void QueryStatistics::incProcessedWatermarks() { this->processedWatermarks++; }
void QueryStatistics::incProcessedTuple(uint64_t tupleCnt) { this->processedTuple += tupleCnt; }

void QueryStatistics::incReceivedBuffers() { this->receivedBuffers++; }
void QueryStatistics::incSentBuffers() { this->sentBuffers++; }
void QueryStatistics::incReceivedBytes(uint64_t numBytes) { this->receivedBytes += numBytes; }
void QueryStatistics::incSentBytes(uint64_t numBytes) { this->sentBytes += numBytes; }
// getters too later

void QueryStatistics::setProcessedBuffers(const std::atomic<uint64_t>& processedBuffers) {
    this->processedBuffers = processedBuffers.load();
}

std::string QueryStatistics::getQueryStatisticsAsString() {
    std::stringstream ss;
    ss << "processedTasks=" << processedTasks;
    ss << " processedTuple=" << processedTuple;
    ss << " processedBuffers=" << processedBuffers;
    ss << " processedWatermarks=" << processedWatermarks;
    ss << " receivedBuffers=" << receivedBuffers;
    ss << " sentBuffers=" << sentBuffers;
    ss << " receivedBytes=" << receivedBytes;
    ss << " sentBytes=" << sentBytes;
    // add too
    return ss.str();
}

void QueryStatistics::destroy() {
    std::scoped_lock locks(queryMutex, workMutex, statisticsMutex);
    // NES_DEBUG("QueryStatistics: Destroy Task Queue " << taskQueue.size());
    // taskQueue.clear();
    // NES_DEBUG("QueryStatistics: Destroy queryId_to_query_map " << operatorIdToQueryMap.size());
    //
    // operatorIdToQueryMap.clear();
    // queryToStatisticsMap.clear();
    //
    // NES_DEBUG("QueryStatistics::resetQueryManager finished");
}
}// namespace NES::NodeEngine
