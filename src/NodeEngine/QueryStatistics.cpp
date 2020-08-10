#include <NodeEngine/QueryStatistics.hpp>
#include <sstream>
namespace NES {

const std::atomic<size_t>& QueryStatistics::getProcessedTasks() const {
    return processedTasks;
}

const std::atomic<size_t>& QueryStatistics::getProcessedTuple() const {
    return processedTuple;
}

const std::atomic<size_t>& QueryStatistics::getProcessedBuffers() const {
    return processedBuffers;
}

const std::atomic<size_t>& QueryStatistics::getProcessedWaterMarks() const {
    return processedWaterMarks;
}

void QueryStatistics::setProcessedTasks(const std::atomic<size_t>& processedTasks) {
    this->processedTasks = processedTasks.load();
}

void QueryStatistics::setProcessedTuple(const std::atomic<size_t>& processedTuple) {
    this->processedTuple = processedTuple.load();
}

void QueryStatistics::incProcessedBuffers() {
    this->processedBuffers++;
}

void QueryStatistics::incProcessedTasks() {
    this->processedTasks++;
}

void QueryStatistics::incProcessedWaterMarks() {
    this->processedWaterMarks++;
}
void QueryStatistics::incProcessedTuple(size_t tupleCnt) {
    this->processedTuple += tupleCnt;
}

void QueryStatistics::setProcessedBuffers(const std::atomic<size_t>& processedBuffers) {
    this->processedBuffers = processedBuffers.load();
}

std::string QueryStatistics::getQueryStatisticsAsString() {
    std::stringstream ss;
    ss << "processedTasks=" << processedTasks;
    ss << " processedTuple=" << processedTuple;
    ss << " processedBuffers=" << processedBuffers;
    ss << " processedWaterMarks=" << processedWaterMarks;
    return ss.str();
}
}

