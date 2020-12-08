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

void QueryStatistics::setProcessedBuffers(const std::atomic<uint64_t>& processedBuffers) {
    this->processedBuffers = processedBuffers.load();
}

std::string QueryStatistics::getQueryStatisticsAsString() {
    std::stringstream ss;
    ss << "processedTasks=" << processedTasks;
    ss << " processedTuple=" << processedTuple;
    ss << " processedBuffers=" << processedBuffers;
    ss << " processedWatermarks=" << processedWatermarks;
    return ss.str();
}
}// namespace NES
