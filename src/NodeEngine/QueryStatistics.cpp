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

uint64_t QueryStatistics::getProcessedTasks() const { return processedTasks.load(); }

uint64_t QueryStatistics::getProcessedTuple() const { return processedTuple.load(); }

uint64_t QueryStatistics::getProcessedBuffers() const { return processedBuffers.load(); }

uint64_t QueryStatistics::getProcessedWatermarks() const { return processedWatermarks.load(); }

uint64_t QueryStatistics::getLatencySum() const { return latencySum.load(); }

uint64_t QueryStatistics::getQueueSizeSum() const { return queueSizeSum.load(); }

void QueryStatistics::setProcessedTasks(uint64_t processedTasks) {
    this->processedTasks = processedTasks;
}

void QueryStatistics::setProcessedTuple(uint64_t processedTuple) {
    this->processedTuple = processedTuple;
}

void QueryStatistics::incProcessedBuffers() { this->processedBuffers++; }

void QueryStatistics::incProcessedTasks() { this->processedTasks++; }

void QueryStatistics::incProcessedWatermarks() { this->processedWatermarks++; }
void QueryStatistics::incProcessedTuple(uint64_t tupleCnt) { this->processedTuple += tupleCnt; }
void QueryStatistics::incLatencySum(uint64_t latency) { this->latencySum += latency; }
void QueryStatistics::incQueueSizeSum(uint64_t size){ this->queueSizeSum += size; }

void QueryStatistics::setProcessedBuffers(uint64_t processedBuffers) {
    this->processedBuffers = processedBuffers;
}

void QueryStatistics::addTimestampToLatencyValue(uint64_t now, uint64_t latency) { tsToLatencyMap[now].push_back(latency); }

std::map<uint64_t, std::vector<uint64_t>> QueryStatistics::getTsToLatencyMap() { return tsToLatencyMap; }

std::string QueryStatistics::getQueryStatisticsAsString() {
    std::stringstream ss;
    ss << "queryId=" << queryId.load();
    ss << " subPlanId=" << subQueryId.load();
    ss << " processedTasks=" << processedTasks.load();
    ss << " processedTuple=" << processedTuple.load();
    ss << " processedBuffers=" << processedBuffers.load();
    ss << " processedWatermarks=" << processedWatermarks.load();
    ss << " latencySum=" << latencySum.load();
    ss << " queueSizeSum=" << queueSizeSum.load();
    return ss.str();
}
uint64_t QueryStatistics::getQueryId() const { return queryId.load(); }
uint64_t QueryStatistics::getSubQueryId() const { return subQueryId.load(); }
}// namespace NES::NodeEngine
